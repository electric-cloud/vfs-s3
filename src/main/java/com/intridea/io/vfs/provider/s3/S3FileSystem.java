
// S3FileSystem.java --
//
// S3FileSystem.java is part of ElectricCommander.
//
// Copyright (c) 2005-2015 Electric Cloud, Inc.
// All rights reserved.
//

package com.intridea.io.vfs.provider.s3;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import com.amazonaws.AmazonServiceException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Region;

/**
 * An S3 file system.
 *
 * @author  Marat Komarov
 * @author  Matthias L. Jugel
 * @author  Moritz Siuts
 */
public class S3FileSystem
    extends AbstractFileSystem
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Log logger = LogFactory.getLog(S3FileSystem.class);

    //~ Instance fields --------------------------------------------------------

    private final AmazonS3Client service;
    private final Bucket         bucket;
    private boolean              shutdownServiceOnClose = false;

    //~ Constructors -----------------------------------------------------------

    public S3FileSystem(
            S3FileName        fileName,
            AmazonS3Client    service,
            FileSystemOptions fileSystemOptions)
        throws FileSystemException
    {
        super(fileName, null, fileSystemOptions);

        String bucketId = fileName.getBucketId();

        this.service = service;

        try {

            if (service.doesBucketExist(bucketId)) {
                bucket = new Bucket(bucketId);
            }
            else {
                bucket = service.createBucket(bucketId);

                if (logger.isDebugEnabled()) {
                    logger.debug("Created new bucket.");
                }
            }

            logger.info("Created new S3 FileSystem " + bucketId);
        }
        catch (AmazonServiceException e) {
            throw new FileSystemException(
                "vfs.provider.s3/creation_failure.error", e, bucketId,
                e.getMessage());
        }
    }

    //~ Methods ----------------------------------------------------------------

    @Override protected void addCapabilities(Collection<Capability> caps)
    {
        caps.addAll(S3FileProvider.capabilities);
    }

    @Override protected FileObject createFile(AbstractFileName fileName)
        throws Exception
    {
        return new S3FileObject(fileName, this);
    }

    @Override protected void doCloseCommunicationLink()
    {

        if (shutdownServiceOnClose) {
            service.shutdown();
        }
    }

    protected Bucket getBucket()
    {
        return bucket;
    }

    protected Region getRegion()
    {
        return S3FileSystemConfigBuilder.getInstance()
                                        .getRegion(getFileSystemOptions());
    }

    protected AmazonS3 getService()
    {
        return service;
    }

    public void setShutdownServiceOnClose(boolean shutdownServiceOnClose)
    {
        this.shutdownServiceOnClose = shutdownServiceOnClose;
    }
}
