
// S3FileObject.java --
//
// S3FileObject.java is part of ElectricCommander.
//
// Copyright (c) 2005-2016 Electric Cloud, Inc.
// All rights reserved.
//

package com.intridea.io.vfs.provider.s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.LockByFileStrategyFactory;
import org.apache.commons.vfs2.util.MonitorOutputStream;

import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;

import com.amazonaws.auth.AWSCredentials;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import com.intridea.io.vfs.operations.Acl;
import com.intridea.io.vfs.operations.Acl.Group;
import com.intridea.io.vfs.operations.IAclGetter;

import static java.nio.channels.Channels.newInputStream;
import static java.util.Calendar.SECOND;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.apache.commons.vfs2.FileName.SEPARATOR;
import static org.apache.commons.vfs2.NameScope.CHILD;
import static org.apache.commons.vfs2.NameScope.DESCENDENT_OR_SELF;
import static org.apache.commons.vfs2.NameScope.FILE_SYSTEM;

import static com.amazonaws.services.s3.model.ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION;

import static com.intridea.io.vfs.operations.Acl.Permission.READ;
import static com.intridea.io.vfs.operations.Acl.Permission.WRITE;
import static com.intridea.io.vfs.provider.s3.AmazonS3ClientHack.extractCredentials;

/**
 * Implementation of the virtual S3 file system object using the AWS-SDK.
 *
 * <p>Based on Matthias Jugel code.
 * http://thinkberg.com/svn/moxo/trunk/modules/vfs.s3/</p>
 *
 * @author  Marat Komarov
 * @author  Matthias L. Jugel
 * @author  Moritz Siuts
 * @author  Shon Vella
 */
@SuppressWarnings(
    {
        "CastToConcreteClass",
        "deprecation",
        "InstanceofInterfaces",
        "IOResourceOpenedButNotSafelyClosed",
        "resource"
    }
)
public class S3FileObject
    extends AbstractFileObject<S3FileSystem>
{

    //~ Static fields/initializers ---------------------------------------------

    @NonNls private static final Log log = LogFactory.getLog(
            S3FileObject.class);

    //
    @NonNls private static final String MIMETYPE_JETS3T_DIRECTORY =
        "application/x-directory";

    //
    private static final byte[]  ZERO_LENGTH_ARRAY = new byte[0];
    private static final Pattern SLASH_PATTERN     = Pattern.compile("/");

    /** Number of bytes we'll store on disk in the cache. */
    private static final long CACHE_SIZE;

    /**
     * Cache concurrency level. Four is the default concurrency -- we're
     * defining it explicitly so we can define MAX_CACHEABLE_SIZE.
     */
    private static final int CACHE_CONCURRENCY = 4;

    /**
     * Largest artifact we can store in the cache (the cache is segmented for
     * concurrency, and each segment can store at most
     * CACHE_SIZE/CACHE_CONCURRENCY bytes.
     */
    private static final long MAX_CACHEABLE_SIZE;

    /** Number of seconds before evicting and idle cache entry. */
    private static final long CACHE_IDLE_TIMEOUT;

    static {

        // Default to 1GB
        CACHE_SIZE         = Long.getLong("S3_CACHE_SIZE", 1024) * 1024 * 1024;
        MAX_CACHEABLE_SIZE = CACHE_SIZE / CACHE_CONCURRENCY;

        // Default to 1 hour
        CACHE_IDLE_TIMEOUT = Long.getLong("S3_CACHE_IDLE_TIMEOUT", 3600);

        //
        log.info("S3_CACHE_SIZE: " + CACHE_SIZE);
        log.info("SE_CACHE_IDLE_TIMEOUT: " + CACHE_IDLE_TIMEOUT);
    }

    /** Cache of File objects. */
    private static final Cache<String, File> FILE_CACHE = CacheBuilder
            .<String, File>newBuilder()
            .expireAfterAccess(CACHE_IDLE_TIMEOUT, SECONDS) // Idle out old content
            .concurrencyLevel(CACHE_CONCURRENCY)            // Define how many segment the cache will have
            .maximumWeight(CACHE_SIZE)                      // Uses the file size to limit cache contents.
            .weigher(new Weigher<String, File>() {
                    @Override public int weigh(
                            @NotNull String key,
                            @NotNull File   value)
                    {

                        // Return the size of the file, pinned to max int value
                        // since the return value has to be an int
                        long length = value.length();

                        if (log.isDebugEnabled()) {
                            log.debug(
                                "S3Cache weigher: " + value.getName() + " is "
                                    + length
                                    + " bytes");
                        }

                        return Long.valueOf(Math.min(length, Integer.MAX_VALUE))
                                   .intValue();
                    }
                })
            .removalListener(notification -> {
                File cacheFile = notification.getValue();

                // Don't delete files which are being evicted because
                // they're too large for the cache.
                if (cacheFile != null) {

                    if (log.isDebugEnabled()) {
                        log.debug(
                            "S3Cache evicting: " + cacheFile.getName() + " at "
                            + cacheFile.length() + " bytes");
                    }

                    if (!cacheFile.delete()) {
                        log.error(
                            "S3Cache evicting: Unable to delete temporary file: "
                            + cacheFile.getPath());
                    }
                }
            })
            .build();

    //~ Instance fields --------------------------------------------------------

    /** Amazon S3 object. */
    @Nullable private volatile ObjectMetadata m_objectMetadata;
    private volatile String                   m_objectKey;

    /** True when content attached to file. */
    private volatile boolean m_attached;
    private volatile Owner   m_fileOwner;

    /** Set while the output stream is in use. */
    @Nullable private volatile File m_outputFile;

    /**
     * Used when file is too large to fit into the cache. Needs to be deleted
     * when the content is closed since it's not cache-managed.
     */
    @Nullable private volatile File m_uncacheableFile;

    //~ Constructors -----------------------------------------------------------

    S3FileObject(
            AbstractFileName      fileName,
            @NotNull S3FileSystem fileSystem)
    {
        super(fileName, fileSystem, new LockByFileStrategyFactory());
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Queries the object if a simple rename to the filename of {@code newfile}
     * is possible.
     *
     * @param   newfile  the new filename
     *
     * @return  true if rename is possible
     */
    @Override public boolean canRenameTo(FileObject newfile)
    {
        return false;
    }

    /**
     * Copies another file to this file.
     *
     * @param   file      The FileObject to copy.
     * @param   selector  The FileSelector.
     *
     * @throws  FileSystemException  if an error occurs.
     */
    @Override
    @SuppressWarnings({"OverlyComplexMethod", "OverlyLongMethod"})
    public void copyFrom(
            @NotNull @SuppressWarnings("ParameterHidesMemberVariable")
            FileObject          file,
            FileSelector        selector)
        throws FileSystemException
    {

        if (!file.exists()) {
            throw new FileSystemException(
                "vfs.provider/copy-missing-file.error", file);
        }

        // Locate the files to copy across
        List<FileObject> files = new ArrayList<>();

        file.findFiles(selector, false, files);

        // Copy everything across
        for (FileObject srcFile : files) {

            // Determine the destination file
            String       relPath  = file.getName()
                                        .getRelativeName(srcFile.getName());
            S3FileObject destFile = (S3FileObject) resolveFile(relPath,
                    DESCENDENT_OR_SELF);

            // Clean up the destination file, if necessary
            if (destFile.exists()) {

                if (destFile.getType() != srcFile.getType()) {

                    // The destination file exists, and is not of the same type,
                    // so delete it TODO - add a pluggable policy for deleting
                    // and overwriting existing files
                    destFile.delete(Selectors.SELECT_ALL);
                }
            }
            else {
                FileObject parent = getParent();

                if (parent != null) {
                    parent.createFolder();
                }
            }

            // Copy across
            // noinspection OverlyBroadCatchBlock
            try {

                if (srcFile.getType()
                           .hasChildren()) {
                    destFile.createFolder();
                    // do server side copy if both source and dest are in S3 and
                    // using same credentials
                }
                else if (srcFile instanceof S3FileObject) {
                    S3FileObject      s3SrcFile      = (S3FileObject) srcFile;
                    String            srcBucketName  = s3SrcFile.getBucket()
                                                                .getName();
                    String            srcFileName    = s3SrcFile.getS3Key();
                    String            destBucketName = destFile.getBucket()
                                                               .getName();
                    String            destFileName   = destFile.getS3Key();
                    CopyObjectRequest copy           = new CopyObjectRequest(
                            srcBucketName, srcFileName, destBucketName,
                            destFileName);

                    if (srcFile.getType() == FileType.FILE
                            && getServerSideEncryption()) {
                        ObjectMetadata meta = s3SrcFile.getObjectMetadata();

                        meta.setSSEAlgorithm(AES_256_SERVER_SIDE_ENCRYPTION);
                        copy.setNewObjectMetadata(meta);
                    }

                    getService().copyObject(copy);
                }
                else if (srcFile.getType()
                                .hasContent()
                        && "file".equals(srcFile.getURL()
                                                .getProtocol())) {

                    // do direct upload from file to avoid overhead of making
                    // a copy of the file
                    try {
                        File srcUriFile = new File(srcFile.getURL()
                                                          .toURI());

                        destFile.upload(srcUriFile);
                    }
                    catch (URISyntaxException e) {

                        // couldn't convert URL to URI, but should still be
                        // able to do the slower way
                        super.copyFrom(file, selector);
                    }
                }
                else {
                    super.copyFrom(file, selector);
                }
            }
            catch (@NotNull IOException | AmazonClientException e) {
                throw new FileSystemException("vfs.provider/copy-file.error", e,
                    srcFile, destFile);
            }
            finally {
                destFile.close();
            }
        }
    }

    @Override
    @SuppressWarnings("NonFinalFieldReferenceInEquals")
    public boolean equals(Object obj)
    {

        if (obj == this) {
            return true;
        }

        if (!S3FileObject.class.isInstance(obj)) {
            return false;
        }

        S3FileObject that = (S3FileObject) obj;

        return Objects.equals(m_objectKey, that.m_objectKey);
    }

    @Override
    @SuppressWarnings("NonFinalFieldReferencedInHashCode")
    public int hashCode()
    {
        return super.hashCode()
            + (m_objectKey == null
                ? 37
                : m_objectKey.hashCode());
    }

    @Override
    @SuppressWarnings("OverlyLongMethod")
    protected void doAttach()
    {

        if (m_attached) {
            return;
        }

        try {

            // Do we have file with name?
            String candidateKey = getS3Key();

            m_objectMetadata = getService().getObjectMetadata(getBucket()
                        .getName(), candidateKey);
            m_objectKey      = candidateKey;
            m_attached       = true;

            log.info("Attach file to S3 Object: " + m_objectKey);

            return;
        }
        catch (AmazonServiceException e) {

            if (log.isDebugEnabled()) {

                // No, we don't
                log.debug("doAttach: No file with name " + getName()
                        + ": " + e.getMessage());
            }
        }
        catch (AmazonClientException e) {

            if (log.isDebugEnabled()) {

                // No, we don't
                // We are attempting to attach to the root bucket
                log.debug("doAttach: Attempting to attach to root bucket: "
                        + e.getMessage());
            }
        }

        try {

            // Do we have folder with that name?
            String candidateKey = getS3Key() + SEPARATOR;

            m_objectMetadata = getService().getObjectMetadata(getBucket()
                        .getName(), candidateKey);
            m_objectKey      = candidateKey;
            m_attached       = true;

            log.info("Attach folder to S3 Object: " + m_objectKey);

            return;
        }
        catch (AmazonServiceException e) {

            if (log.isDebugEnabled()) {

                // No, we don't
                log.debug("doAttach: No folder with that name "
                        + getName()
                        + ": " + e.getMessage());
            }
        }

        // Create a new
        if (m_objectMetadata == null) {
            m_objectMetadata = new ObjectMetadata();
            m_objectKey      = getS3Key();

            ObjectMetadata objectMetaData = m_objectMetadata;

            assert objectMetaData != null;
            objectMetaData.setLastModified(new Date());
            m_attached = true;
            log.info("Attach new S3 Object: " + m_objectKey);
        }
    }

    @Override protected void doCreateFolder()
        throws Exception
    {

        if (log.isDebugEnabled()) {
            log.debug("Create new folder in bucket ["
                    + (getBucket() != null
                        ? getBucket().getName()
                        : "null")
                    + "] with key ["
                    + (m_objectMetadata != null
                        ? m_objectKey
                        : "null")
                    + "]");
        }

        if (m_objectMetadata == null) {
            return;
        }

        InputStream    input    = new ByteArrayInputStream(ZERO_LENGTH_ARRAY);
        ObjectMetadata metadata = new ObjectMetadata();

        metadata.setContentLength(0);

        if (getServerSideEncryption()) {
            metadata.setSSEAlgorithm(AES_256_SERVER_SIDE_ENCRYPTION);
        }

        String dirName = m_objectKey.endsWith(SEPARATOR)
            ? m_objectKey
            : m_objectKey + SEPARATOR;

        log.info("Creating folder " + getName());
        getService().putObject(new PutObjectRequest(getBucket().getName(),
                dirName, input, metadata));
    }

    @Override protected void doDelete()
        throws Exception
    {
        log.info("Deleting " + getName());
        getService().deleteObject(getBucket().getName(), m_objectKey);
    }

    @Override protected void doDetach()
        throws Exception
    {

        if (m_attached) {
            log.info("Detach from S3 Object: " + m_objectKey);
            m_objectMetadata = null;
            m_attached       = false;

            File uncacheableFile = m_uncacheableFile;

            // If the file was too large to fit in the cache, delete it now.
            if (uncacheableFile != null) {

                if (uncacheableFile.delete()) {
                    log.info(
                        "S3Cache doDetach: Deleted too-large-to-cache file: "
                            + uncacheableFile.getPath());
                }
                else {
                    log.error("Unable to delete temporary file: "
                            + uncacheableFile.getPath());
                }
            }

            m_uncacheableFile = null;
        }
    }

    @Override protected long doGetContentSize()
        throws Exception
    {
        ObjectMetadata objectMetadata = m_objectMetadata;

        assert objectMetadata != null;

        return objectMetadata.getContentLength();
    }

    @NotNull @Override protected InputStream doGetInputStream()
        throws Exception
    {
        download();

        return newInputStream(getCacheFileChannel());
    }

    @Override protected long doGetLastModifiedTime()
        throws Exception
    {
        ObjectMetadata objectMetadata = m_objectMetadata;

        assert objectMetadata != null;

        return objectMetadata.getLastModified()
                             .getTime();
    }

    @NotNull @Override protected OutputStream doGetOutputStream(
            boolean bAppend)
        throws Exception
    {

        // Will be used by S3OutputStream to upload the file to S3
        File outputFile = createS3TempFile();

        m_outputFile = outputFile;

        // noinspection ChannelOpenedButNotSafelyClosed
        WritableByteChannel result = new RandomAccessFile(outputFile, "rw")
                .getChannel();

        return new S3OutputStream(Channels.newOutputStream(result));
    }

    @NotNull @Override protected FileType doGetType()
        throws Exception
    {
        ObjectMetadata objectMetadata = m_objectMetadata;

        assert objectMetadata != null;

        if (objectMetadata.getContentType() == null) {
            return FileType.IMAGINARY;
        }

        if (m_objectKey != null && m_objectKey.isEmpty()
                || isDirectoryPlaceholder()) {
            return FileType.FOLDER;
        }

        return FileType.FILE;
    }

    @NotNull @Override protected String[] doListChildren()
        throws Exception
    {
        @NonNls String path = m_objectKey;

        // make sure we add a '/' slash at the end to find children
        if (path != null && !path.isEmpty() && !path.endsWith(SEPARATOR)) {
            path += "/";
        }

        ListObjectsRequest loReq = new ListObjectsRequest();

        loReq.setBucketName(getBucket().getName());
        loReq.setDelimiter("/");
        loReq.setPrefix(path);

        ObjectListing               listing        = getService().listObjects(
                loReq);
        Collection<S3ObjectSummary> summaries      = new ArrayList<>(
                listing.getObjectSummaries());
        Collection<String>          commonPrefixes = new TreeSet<>(
                listing.getCommonPrefixes());

        while (listing.isTruncated()) {
            listing = getService().listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
            commonPrefixes.addAll(listing.getCommonPrefixes());
        }

        List<String> childrenNames = new ArrayList<>(summaries.size()
                    + commonPrefixes.size());

        // add the prefixes (non-empty subdirs) first
        for (String commonPrefix : commonPrefixes) {

            // strip path from name (leave only base name)
            assert path != null;

            String stripPath = commonPrefix.substring(path.length());

            childrenNames.add(stripPath);
        }

        for (S3ObjectSummary summary : summaries) {

            if (!summary.getKey()
                        .equals(path)) {

                // strip path from name (leave only base name)
                assert path != null;

                String stripPath = summary.getKey()
                                          .substring(path.length());

                childrenNames.add(stripPath);
            }
        }

        return childrenNames.toArray(new String[childrenNames.size()]);
    }

    /**
     * Lists the children of this file. Is only called if {@link #doGetType}
     * returns {@link FileType#FOLDER}. The return value of this method is
     * cached, so the implementation can be expensive.<br>
     * Other than {@code doListChildren} you could return FileObject's to e.g.
     * reinitialize the type of the file.<br>
     * (Introduced for WebDAV: "permission denied on resource" during getType())
     *
     * @return  The children of this FileObject.
     *
     * @throws  Exception  if an error occurs.
     */
    @NotNull @Override
    @SuppressWarnings({"OverlyComplexMethod", "OverlyLongMethod"})
    protected FileObject[] doListChildrenResolved()
        throws Exception
    {
        @NonNls String path = m_objectKey;

        // make sure we add a '/' slash at the end to find children
        if (path != null && !path.isEmpty() && !path.endsWith(SEPARATOR)) {
            path += "/";
        }

        ListObjectsRequest loReq = new ListObjectsRequest();

        loReq.setBucketName(getBucket().getName());
        loReq.setDelimiter("/");
        loReq.setPrefix(path);

        ObjectListing               listing        = getService().listObjects(
                loReq);
        Collection<S3ObjectSummary> summaries      = new ArrayList<>(
                listing.getObjectSummaries());
        Collection<String>          commonPrefixes = new TreeSet<>(
                listing.getCommonPrefixes());

        while (listing.isTruncated()) {
            listing = getService().listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
            commonPrefixes.addAll(listing.getCommonPrefixes());
        }

        List<FileObject> resolvedChildren = new ArrayList<>(summaries.size()
                    + commonPrefixes.size());

        // add the prefixes (non-empty subdirs) first
        for (String commonPrefix : commonPrefixes) {

            // strip path from name (leave only base name)
            assert path != null;

            @NonNls String stripPath   = commonPrefix.substring(path.length());
            FileObject     childObject = resolveFile(stripPath,
                    "/".equals(stripPath)
                        ? FILE_SYSTEM
                        : CHILD);

            if (childObject instanceof S3FileObject && !"/".equals(stripPath)) {
                resolvedChildren.add(childObject);
            }
        }

        for (S3ObjectSummary summary : summaries) {

            if (!summary.getKey()
                        .equals(path)) {

                // strip path from name (leave only base name)
                assert path != null;

                String     stripPath   = summary.getKey()
                                                .substring(path.length());
                FileObject childObject = resolveFile(stripPath, CHILD);

                if (childObject instanceof S3FileObject) {
                    S3FileObject   s3FileObject  = (S3FileObject) childObject;
                    ObjectMetadata childMetadata = new ObjectMetadata();

                    childMetadata.setContentLength(summary.getSize());
                    childMetadata.setContentType(Mimetypes.getInstance()
                                                          .getMimetype(
                                                              s3FileObject
                                .getName()
                                .getBaseName()));
                    childMetadata.setLastModified(summary.getLastModified());
                    childMetadata.setHeader(Headers.ETAG, summary.getETag());
                    s3FileObject.m_objectMetadata = childMetadata;
                    s3FileObject.m_objectKey      = summary.getKey();
                    s3FileObject.m_attached       = true;

                    resolvedChildren.add(s3FileObject);
                }
            }
        }

        return resolvedChildren.toArray(
            new FileObject[resolvedChildren.size()]);
    }

    @Override protected boolean doSetLastModifiedTime(long modtime)
        throws Exception
    {
        ObjectMetadata objectMetadata = m_objectMetadata;

        assert objectMetadata != null;

        long    oldModified           = objectMetadata.getLastModified()
                                                      .getTime();
        boolean differentModifiedTime = oldModified != modtime;

        if (differentModifiedTime) {
            objectMetadata.setLastModified(new Date(modtime));
        }

        return differentModifiedTime;
    }

    @NotNull private File createS3TempFile()
        throws IOException
    {
        File result = File.createTempFile("s3." + makeSafePath(getName())
                    + ".", "");

        result.deleteOnExit();

        return result;
    }

    /**
     * Creates an executor service for use with a TransferManager. This allows
     * us to control the maximum number of threads used because for the
     * TransferManager default of 10 is way too many.
     *
     * @return  an executor service
     */
    private ExecutorService createTransferManagerExecutorService()
    {
        int           maxThreads    = S3FileSystemConfigBuilder.getInstance()
                                                               .getMaxUploadThreads(
                                                                   getFileSystem()
                                                                   .getFileSystemOptions());
        ThreadFactory threadFactory = new ThreadFactory() {
            private int m_threadCount = 1;

            @NotNull @Override public Thread newThread(@NotNull Runnable r)
            {
                Thread thread = new Thread(r);

                thread.setName("s3-upload-" + getName().getBaseName() + "-"
                        + m_threadCount++);

                return thread;
            }
        };

        return Executors.newFixedThreadPool(maxThreads, threadFactory);
    }

    @NotNull private File doDownload(@NotNull File tempFile)
        throws FileSystemException
    {

        if (log.isDebugEnabled()) {
            log.debug("downloading " + getName());
        }

        String objectPath = getName().getPath();

        try(S3Object obj = getService().getObject(getBucket().getName(),
                        m_objectKey)) {
            log.info("Downloading S3 Object: " + objectPath);

            if (obj.getObjectMetadata()
                   .getContentLength() > 0) {
                InputStream is = obj.getObjectContent();

                try(ReadableByteChannel rbc = Channels.newChannel(is);
                        FileChannel cacheFc =
                            new RandomAccessFile(tempFile, "rw").getChannel()) {
                    cacheFc.transferFrom(rbc, 0,
                        obj.getObjectMetadata()
                           .getContentLength());
                }
            }

            return tempFile;
        }
        catch (@NotNull AmazonServiceException | IOException e) {
            throw new FileSystemException(
                "vfs.provider.s3/download_failure.error", e, objectPath,
                e.getMessage());
        }
    }

    /**
     * Download S3 object content and save it in temporary file. Do it only if
     * object was not already downloaded.
     *
     * @throws  FileSystemException
     */
    private void download()
        throws FileSystemException
    {

        // set if the file is too large to fit in the cache.
        File uncacheablefile = m_uncacheableFile;

        if (uncacheablefile != null) {

            if (uncacheablefile.exists()) {

                // Got our file
                return;
            }
            else {

                // File was deleted by someone out of the file system -- treat
                // as cache miss.
                m_uncacheableFile = null;
            }
        }

        // Bit of a hack to turn a deleted cache file into a cache invalidation
        // and then a cache miss.
        File cacheFile = FILE_CACHE.getIfPresent(m_objectKey);

        if (cacheFile != null && !cacheFile.exists()) {
            FILE_CACHE.invalidate(m_objectKey);
        }

        // If the file is too large to cache, download directly.
        long artifactLength = getArtifactLength();

        if (artifactLength > MAX_CACHEABLE_SIZE) {

            try {
                File tempFile = createS3TempFile();

                log.warn("S3Cache download: " + tempFile.getName()
                        + " is too large to fit in the cache: " + artifactLength
                        + " bytes");
                doDownload(tempFile);
                m_uncacheableFile = tempFile;

                return;
            }
            catch (IOException e) {
                throw new FileSystemException(e.toString(), e);
            }
        }

        // Use the file cache to make sure we download atomically -- only once
        // for each cache file.
        try {
            FILE_CACHE.get(m_objectKey, () -> doDownload(createS3TempFile()));
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (FileSystemException.class.isInstance(cause)) {
                throw FileSystemException.class.cast(cause);
            }
            else {
                throw new FileSystemException(cause.toString(), cause);
            }
        }
    }

    /**
     * Put S3 ACL list.
     *
     * @param  s3Acl  acl list
     */
    private void putS3Acl(AccessControlList s3Acl)
    {
        @NonNls String key = getS3Key();

        // Determine context. Object or Bucket
        if (key != null && key.isEmpty()) {
            getService().setBucketAcl(getBucket().getName(), s3Acl);
        }
        else {

            // Before any operations with object it must be attached
            doAttach();

            // Put ACL to S3
            getService().setObjectAcl(getBucket().getName(), m_objectKey,
                s3Acl);
        }
    }

    /**
     * Uploads File to S3.
     *
     * @param   src  the File
     *
     * @throws  IOException
     * @throws  InterruptedIOException
     */
    private void upload(@NotNull File src)
        throws IOException
    {
        PutObjectRequest request = new PutObjectRequest(getBucket().getName(),
                getS3Key(), src);
        ObjectMetadata   md      = new ObjectMetadata();

        md.setContentLength(src.length());
        md.setContentType(Mimetypes.getInstance()
                                   .getMimetype(getName().getBaseName()));

        // set encryption if needed
        if (getServerSideEncryption()) {
            md.setSSEAlgorithm(AES_256_SERVER_SIDE_ENCRYPTION);
        }

        request.setMetadata(md);

        // noinspection OverlyBroadCatchBlock
        try {
            TransferManagerConfiguration tmConfig =
                new TransferManagerConfiguration();

            // if length is below multi-part threshold, just use put,
            // otherwise create and use a TransferManager
            if (md.getContentLength()
                    < tmConfig.getMultipartUploadThreshold()) {
                log.info("Putting " + src.getAbsolutePath() + " to "
                        + getName());
                getService().putObject(request);
            }
            else {
                log.info("Transferring " + src.getAbsolutePath() + " to "
                        + getName());

                TransferManager transferManager = new TransferManager(
                        getService(), createTransferManagerExecutorService());

                try {
                    Upload upload = transferManager.upload(request);

                    upload.waitForCompletion();
                }
                finally {
                    transferManager.shutdownNow(false);
                }
            }

            doDetach();
            doAttach();
        }
        catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        }
        catch (IOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Returns access control list for this file.
     *
     * <p>VFS interfaces doesn't provide interface to manage permissions. ACL
     * can be accessed through {@link FileObject#getFileOperations()} Sample:
     * {@code file.getFileOperations().getOperation(IAclGetter.class)}</p>
     *
     * @return  Current Access control list for a file
     *
     * @throws  FileSystemException
     *
     * @see     {@link FileObject#getFileOperations()}
     * @see     {@link IAclGetter}
     */
    @NotNull
    @SuppressWarnings({"OverlyComplexMethod", "OverlyLongMethod"})
    public Acl getAcl()
        throws FileSystemException
    {

        if (log.isDebugEnabled()) {
            log.debug("getAcl " + getName());
        }

        Acl               myAcl = new Acl();
        AccessControlList s3Acl;

        try {
            s3Acl = getS3Acl();
        }
        catch (AmazonServiceException e) {
            throw new FileSystemException("vfs.provider.s3/acl_failure.error",
                e, getName(), e.getMessage());
        }

        // Get S3 file owner
        Owner owner = s3Acl.getOwner();

        m_fileOwner = owner;

        // Read S3 ACL list and build VFS ACL.
        Set<Grant> grants = s3Acl.getGrants();

        for (Grant item : grants) {

            // Map enums to jets3t ones
            Permission       perm   = item.getPermission();
            Acl.Permission[] rights;

            if (perm == Permission.FullControl) {
                rights = Acl.Permission.values();
            }
            else if (perm == Permission.Read) {
                rights    = new Acl.Permission[1];
                rights[0] = READ;
            }
            else if (perm == Permission.Write) {
                rights    = new Acl.Permission[1];
                rights[0] = WRITE;
            }
            else {

                // Skip unknown permission
                log.error(String.format("Skip unknown permission %s", perm)); // NON-NLS

                continue;
            }

            // Set permissions for groups
            // noinspection ChainOfInstanceofChecks
            if (item.getGrantee() instanceof GroupGrantee) {
                GroupGrantee grantee = (GroupGrantee) item.getGrantee();

                if (grantee == GroupGrantee.AllUsers) {

                    // Allow rights to GUEST
                    myAcl.allow(Group.EVERYONE, rights);
                }
                else if (grantee == GroupGrantee.AuthenticatedUsers) {

                    // Allow rights to AUTHORIZED
                    myAcl.allow(Group.AUTHORIZED, rights);
                }
            }
            else if (item.getGrantee() instanceof CanonicalGrantee) {
                Grantee        grantee    = item.getGrantee();
                @NonNls String identifier = grantee.getIdentifier();

                if (identifier.equals(owner.getId())) {

                    // The same owner and grantee understood as OWNER group
                    myAcl.allow(Group.OWNER, rights);
                }
            }
        }

        return myAcl;
    }

    private long getArtifactLength()
        throws FileSystemException
    {
        String objectPath = getName().getPath();

        try(S3Object obj = getService().getObject(getBucket().getName(),
                        m_objectKey)) {
            return obj.getObjectMetadata()
                      .getContentLength();
        }
        catch (@NotNull AmazonServiceException | IOException e) {
            throw new FileSystemException(
                "vfs.provider.s3/download_failure.error", e, objectPath,
                e.getMessage());
        }
    }

    /**
     * Amazon S3 bucket.
     *
     * @return  amazon S3 bucket.
     */
    private Bucket getBucket()
    {
        return ((S3FileSystem) getFileSystem()).getBucket();
    }

    /**
     * Returns file that was used as local cache. Useful to do something with
     * local tools like image resizing and so on
     *
     * @return  absolute path to file or nul if nothing were downloaded
     */
    @Nullable
    @SuppressWarnings("unused")
    public String getCacheFile()
    {

        if (log.isDebugEnabled()) {
            log.debug("getCacheFile " + getName());
        }

        File uncacheableFile = m_uncacheableFile;

        if (uncacheableFile != null) {

            // We have our own file, bypass the cache
            return uncacheableFile.getAbsolutePath();
        }

        File cacheFile = FILE_CACHE.getIfPresent(m_objectKey);

        if (cacheFile == null) {
            return null;
        }

        return cacheFile.getAbsolutePath();
    }

    /**
     * Get or create temporary file channel for file cache.
     *
     * @return  temporary file channel for file cache
     *
     * @throws  IOException
     */
    private ReadableByteChannel getCacheFileChannel()
        throws IOException
    {
        File cacheFile = getCacheTempFile();

        if (log.isDebugEnabled()) {
            log.debug("getCacheFileChannel "
                    + cacheFile.getAbsolutePath());
        }

        return new RandomAccessFile(cacheFile, "rw").getChannel();
    }

    @NotNull private File getCacheTempFile()
        throws IOException
    {

        // If the file is too large to fit in the cache, we bypass the cache
        File uncacheableFile = m_uncacheableFile;

        if (uncacheableFile != null) {
            return uncacheableFile;
        }

        try {
            return FILE_CACHE.get(m_objectKey, this::createS3TempFile);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (IOException.class.isInstance(cause)) {
                throw IOException.class.cast(cause);
            }
            else {
                throw new IOException(e.toString(), e);
            }
        }
    }

    /**
     * Get direct http url to S3 object.
     *
     * @return  the direct http url to S3 object
     */
    @NotNull public String getHttpUrl()
    {
        @NonNls StringBuilder sb  = new StringBuilder("http://"
                    + getBucket().getName() + ".s3.amazonaws.com/");
        @NonNls String        key = getS3Key();

        // Determine context. Object or Bucket
        if (key != null && key.isEmpty()) {
            return sb.toString();
        }
        else {
            return sb.append(key)
                     .toString();
        }
    }

    /**
     * Get MD5 hash for the file.
     *
     * @return  md5 hash for file
     *
     * @throws  FileSystemException
     */
    @Nullable public String getMD5Hash()
        throws FileSystemException
    {
        String         hash     = null;
        ObjectMetadata metadata = getObjectMetadata();

        if (metadata != null) {
            hash = metadata.getETag(); // TODO this is something different
                                       // than mentioned in method name /
                                       // javadoc
        }

        return hash;
    }

    ObjectMetadata getObjectMetadata()
        throws FileSystemException
    {

        try {
            return getService().getObjectMetadata(getBucket().getName(),
                getS3Key());
        }
        catch (AmazonServiceException e) {
            throw new FileSystemException(
                "vfs.provider.s3/object_metadata_failure.error", e,
                e.getMessage());
        }
    }

    /**
     * Get private url with access key and secret key.
     *
     * @return  the private url
     *
     * @throws  FileSystemException
     */
    @NotNull public String getPrivateUrl()
        throws FileSystemException
    {
        AWSCredentials awsCredentials = S3FileSystemConfigBuilder.getInstance()
                                                                 .getAWSCredentials(
                                                                     getFileSystem()
                                                                     .getFileSystemOptions());

        if (awsCredentials == null) {
            awsCredentials = extractCredentials(getService());
        }

        if (awsCredentials == null) {
            throw new FileSystemException(
                "vfs.provider.s3/empty_credentials.error");
        }

        return String.format("s3://%s:%s@%s/%s", // NON-NLS
            awsCredentials.getAWSAccessKeyId(),
            awsCredentials.getAWSSecretKey(), getBucket().getName(),
            getS3Key());
    }

    /**
     * Get S3 ACL list.
     *
     * @return  acl list
     */
    private AccessControlList getS3Acl()
    {
        @NonNls String key = getS3Key();

        return key != null && key.isEmpty()
        ? getService().getBucketAcl(getBucket().getName())
        : getService().getObjectAcl(getBucket().getName(), key);
    }

    /**
     * Create an S3 key from a commons-vfs path. This simply strips the slash
     * from the beginning if it exists.
     *
     * @return  the S3 object key
     */
    private String getS3Key()
    {
        return getS3Key(getName());
    }

    /**
     * Returns S3 file owner. Loads it from S3 if needed.
     *
     * @return  S3 file owner.
     */
    private Owner getS3Owner()
    {

        if (log.isDebugEnabled()) {
            log.debug("getS3Owner " + getName());
        }

        if (m_fileOwner == null) {
            AccessControlList s3Acl = getS3Acl();

            m_fileOwner = s3Acl.getOwner();
        }

        return m_fileOwner;
    }

    private boolean getServerSideEncryption()
    {
        return S3FileSystemConfigBuilder.getInstance()
                                        .getServerSideEncryption(getFileSystem()
                                            .getFileSystemOptions());
    }

    private AmazonS3 getService()
    {
        return ((S3FileSystem) getFileSystem()).getService();
    }

    /**
     * Temporary accessible url for object.
     *
     * @param   expireInSeconds  seconds until expiration
     *
     * @return  temporary accessible url for object
     *
     * @throws  FileSystemException
     */
    public String getSignedUrl(int expireInSeconds)
        throws FileSystemException
    {
        Calendar cal = Calendar.getInstance();

        cal.add(SECOND, expireInSeconds);

        try {
            return getService().generatePresignedUrl(getBucket().getName(),
                                   getS3Key(), cal.getTime())
                               .toString();
        }
        catch (AmazonServiceException e) {
            throw new FileSystemException(
                "vfs.provider.s3/generate_presigned_url.error", e,
                e.getMessage());
        }
    }

    /**
     * Same as in Jets3t library, to be compatible.
     *
     * @return  same as in Jets3t library, to be compatible.
     */
    private boolean isDirectoryPlaceholder()
    {

        // Recognize "standard" directory place-holder indications used by
        // Amazon's AWS Console and Panic's Transmit.
        ObjectMetadata objectMetadata = m_objectMetadata;

        assert objectMetadata != null;

        if (m_objectKey.endsWith("/")
                && objectMetadata.getContentLength() == 0) {
            return true;
        }

        // Recognize s3sync.rb directory placeholders by MD5/ETag value.
        if ("d66759af42f282e1ba19144df2d405d0".equals(
                    objectMetadata.getETag())) {
            return true;
        }

        // Recognize place-holder objects created by the Google Storage console
        // or S3 Organizer Firefox extension.
        if (m_objectKey.endsWith("_$folder$")
                && objectMetadata.getContentLength() == 0) {
            return true;
        }

        // Recognize legacy JetS3t directory place-holder objects, only gives
        // accurate results if an object's metadata is populated.
        return objectMetadata.getContentLength() == 0
            && MIMETYPE_JETS3T_DIRECTORY.equals(
                objectMetadata.getContentType());
    }

    /**
     * Returns access control list for this file.
     *
     * <p>VFS interfaces doesn't provide interface to manage permissions. ACL
     * can be accessed through {@link FileObject#getFileOperations()} Sample:
     * {@code file.getFileOperations().getOperation(IAclGetter.class)}</p>
     *
     * @param   acl  the access control list
     *
     * @throws  FileSystemException
     *
     * @see     {@link FileObject#getFileOperations()}
     * @see     {@link IAclGetter}
     */
    @SuppressWarnings({"OverlyComplexMethod", "OverlyLongMethod"})
    public void setAcl(@NotNull Acl acl)
        throws FileSystemException
    {

        // Create empty S3 ACL list
        AccessControlList s3Acl = new AccessControlList();

        // Get file owner
        Owner owner;

        try {
            owner = getS3Owner();
        }
        catch (AmazonServiceException e) {
            throw new FileSystemException(
                "vfs.provider.s3/acl_owner_failure.error", e, e.getMessage());
        }

        s3Acl.setOwner(owner);

        // Iterate over VFS ACL rules and fill S3 ACL list
        Map<Group, Acl.Permission[]> rules     = acl.getRules();
        Acl.Permission[]             allRights = Acl.Permission.values();

        for (Entry<Group, Acl.Permission[]> groupEntry : rules.entrySet()) {
            Acl.Permission[] rights = groupEntry.getValue();

            if (rights.length == 0) {

                // Skip empty rights
                continue;
            }

            // Set permission
            Permission perm;

            if (Arrays.equals(rights, allRights)) {
                perm = Permission.FullControl;
            }
            else if (acl.isAllowed(groupEntry.getKey(), READ)) {
                perm = Permission.Read;
            }
            else if (acl.isAllowed(groupEntry.getKey(), WRITE)) {
                perm = Permission.Write;
            }
            else {
                log.error(String.format("Skip unknown set of rights %s", // NON-NLS
                        Arrays.toString(rights)));

                continue;
            }

            // Set grantee
            Grantee grantee;

            if (groupEntry.getKey() == Group.EVERYONE) {
                grantee = GroupGrantee.AllUsers;
            }
            else if (groupEntry.getKey() == Group.AUTHORIZED) {
                grantee = GroupGrantee.AuthenticatedUsers;
            }
            else if (groupEntry.getKey() == Group.OWNER) {
                grantee = new CanonicalGrantee(owner.getId());
            }
            else {
                log.error(String.format("Skip unknown group %s", // NON-NLS
                        groupEntry.getKey()));

                continue;
            }

            // Grant permission
            s3Acl.grantPermission(grantee, perm);
        }

        // Put ACL to S3
        // noinspection OverlyBroadCatchBlock
        try {
            putS3Acl(s3Acl);
        }
        catch (Exception e) {
            throw new FileSystemException(
                "vfs.provider.s3/set_acl_failure.error", e, e.getMessage());
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Get the path from fileName and make it safe to be used as a temp file
     * name (e.g. replace / with _, etc.).
     *
     * @param   fileName
     *
     * @return  Sanitized filename
     */
    @NonNls @NotNull private static String makeSafePath(
            @NotNull FileName fileName)
    {
        return SLASH_PATTERN.matcher(fileName.getPath())
                            .replaceAll("_");
    }

    private static String getS3Key(@NotNull FileName fileName)
    {
        String path = fileName.getPath();

        if (path != null && path.isEmpty()) {
            return path;
        }
        else {
            assert path != null;

            return path.substring(1);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Special JetS3FileObject output stream. It saves all contents in temporary
     * file, onClose sends contents to S3.
     *
     * @author  Marat Komarov
     */
    private class S3OutputStream
        extends MonitorOutputStream
    {

        //~ Constructors -------------------------------------------------------

        S3OutputStream(OutputStream out)
        {
            super(out);
        }

        //~ Methods ------------------------------------------------------------

        @Override protected void onClose()
            throws IOException
        {
            doAttach();

            File outputFile = m_outputFile;

            assert outputFile != null;

            // Send the bits to S3
            upload(outputFile);

            // If the object is too large to fit into the cache we bypass the
            // cache and the file becomes owned by this individual S3FileObject
            if (outputFile.length() >= MAX_CACHEABLE_SIZE) {
                log.warn("S3Cache upload: " + outputFile.getName()
                        + " is too large to fit in the cache: "
                        + outputFile.length() + " bytes");

                // We own the file now
                m_uncacheableFile = outputFile;
            }
            else {
                log.info("S3Cache upload: putting " + outputFile.getName()
                        + " into cache at " + outputFile.length()
                        + " bytes");

                // Update the cache since the file now exists and has the bits.
                // This needs to happen after the S3 upload because if the file
                // is larger than the cache size the file will be immediately
                // evicted (e.g. deleted from disk).
                FILE_CACHE.put(m_objectKey, outputFile);
            }

            // Don't need this any more
            m_outputFile = null;
        }
    }
}
