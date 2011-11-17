package com.proofpoint.event.collector.combiner;

import org.apache.commons.httpclient.Header;
import org.apache.commons.logging.LogFactory;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.XmlResponsesSaxParser;
import org.jets3t.service.impl.rest.httpclient.HttpMethodAndByteCount;
import org.jets3t.service.impl.rest.httpclient.HttpMethodReleaseInputStream;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.security.ProviderCredentials;
import org.jets3t.service.utils.RestUtils;
import org.jets3t.service.utils.ServiceUtils;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class ExtendedRestS3Service
        extends RestS3Service
{
    private static final org.apache.commons.logging.Log log = LogFactory.getLog(ExtendedRestS3Service.class);

    public ExtendedRestS3Service(ProviderCredentials credentials)
            throws S3ServiceException
    {
        super(credentials);
    }

    public Map<String, Object> multipartUploadPartCopy(MultipartUpload upload, int partNumber,
            String sourceBucketName, String sourceObjectKey,
            Calendar ifModifiedSince, Calendar ifUnmodifiedSince,
            String[] ifMatchTags, String[] ifNoneMatchTags,
            Long byteRangeStart, Long byteRangeEnd,
            String versionId)
            throws ServiceException
    {
        if (log.isDebugEnabled()) {
            log.debug("Multipart Copy Object from " + sourceBucketName + ":" + sourceObjectKey
                    + " to " + upload.getBucketName() + ": " + upload.getObjectKey() + "?partNumber=" + partNumber);
        }

        Map<String, Object> metadata = new HashMap<String, Object>();

        String sourceKey = RestUtils.encodeUrlString(sourceBucketName + "/" + sourceObjectKey);

        if (versionId != null) {
            sourceKey += "?versionId=" + versionId;
        }

        metadata.put(getRestHeaderPrefix() + "copy-source", sourceKey);

        if (ifModifiedSince != null) {
            metadata.put(getRestHeaderPrefix() + "copy-source-if-modified-since",
                    ServiceUtils.formatRfc822Date(ifModifiedSince.getTime()));
            if (log.isDebugEnabled()) {
                log.debug("Only copy object if-modified-since:" + ifModifiedSince);
            }
        }
        if (ifUnmodifiedSince != null) {
            metadata.put(getRestHeaderPrefix() + "copy-source-if-unmodified-since",
                    ServiceUtils.formatRfc822Date(ifUnmodifiedSince.getTime()));
            if (log.isDebugEnabled()) {
                log.debug("Only copy object if-unmodified-since:" + ifUnmodifiedSince);
            }
        }
        if (ifMatchTags != null) {
            String tags = ServiceUtils.join(ifMatchTags, ",");
            metadata.put(getRestHeaderPrefix() + "copy-source-if-match", tags);
            if (log.isDebugEnabled()) {
                log.debug("Only copy object based on hash comparison if-match:" + tags);
            }
        }
        if (ifNoneMatchTags != null) {
            String tags = ServiceUtils.join(ifNoneMatchTags, ",");
            metadata.put(getRestHeaderPrefix() + "copy-source-if-none-match", tags);
            if (log.isDebugEnabled()) {
                log.debug("Only copy object based on hash comparison if-none-match:" + tags);
            }
        }

        if ((byteRangeStart != null) || (byteRangeEnd != null)) {
            if ((byteRangeStart == null) || (byteRangeEnd == null)) {
                throw new IllegalArgumentException("both range start and end must be set");
            }
            String range = String.format("bytes=%s-%s", byteRangeStart, byteRangeEnd);
            metadata.put(getRestHeaderPrefix() + "copy-source-range", range);
            if (log.isDebugEnabled()) {
                log.debug("Copy object range:" + range);
            }
        }

        Map<String, String> requestParameters = new HashMap<String, String>();
        requestParameters.put("partNumber", String.valueOf(partNumber));
        requestParameters.put("uploadId", String.valueOf(upload.getUploadId()));

        HttpMethodAndByteCount methodAndByteCount = performRestPut(
                upload.getBucketName(), upload.getObjectKey(), metadata, requestParameters, null, false);

        XmlResponsesSaxParser.CopyObjectResultHandler handler = getXmlResponseSaxParser()
                .parseCopyObjectResponse(
                        new HttpMethodReleaseInputStream(methodAndByteCount.getHttpMethod()));

        // Release HTTP connection manually. This should already have been done by the
        // HttpMethodReleaseInputStream class, but you can never be too sure...
        methodAndByteCount.getHttpMethod().releaseConnection();

        if (handler.isErrorResponse()) {
            throw new ServiceException(
                    "Copy failed: Code=" + handler.getErrorCode() +
                            ", Message=" + handler.getErrorMessage() +
                            ", RequestId=" + handler.getErrorRequestId() +
                            ", HostId=" + handler.getErrorHostId());
        }

        Map<String, Object> map = new HashMap<String, Object>();

        // Result fields returned when copy is successful.
        map.put("Last-Modified", handler.getLastModified());
        map.put("ETag", handler.getETag());

        // Include response headers in result map.
        map.putAll(convertHeadersToMap(methodAndByteCount.getHttpMethod().getResponseHeaders()));
        map = ServiceUtils.cleanRestMetadataMap(
                map, this.getRestHeaderPrefix(), this.getRestMetadataPrefix());

        return map;
    }

    private static Map<String, Object> convertHeadersToMap(Header[] headers)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; headers != null && i < headers.length; i++) {
            map.put(headers[i].getName(), headers[i].getValue());
        }
        return map;
    }
}
