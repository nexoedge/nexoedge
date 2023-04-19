// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h> // exit()
#include <stdio.h>

#include <glog/logging.h>

#include <boost/timer/timer.hpp>

#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/BucketLifecycleConfiguration.h>
#include <aws/s3/model/PutBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/PutBucketVersioningRequest.h>

#include "../../common/config.hh"
#include "aws_s3.hh"

#define OBJ_PATH_MAX (128)

AwsContainer::AwsContainer(int id, std::string bucketName, std::string region, std::string keyId, std::string key, unsigned long int capacity, std::string endpoint, std::string httpProxyIP, unsigned short httpProxyPort, bool useHttp) :
        Container(id, capacity) {

    _cred = Aws::Auth::AWSCredentials(keyId.c_str(), key.c_str());

    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = region.c_str();
    if (!httpProxyIP.empty()) {
        clientConfig.proxyHost = httpProxyIP.c_str();
        clientConfig.proxyPort = httpProxyPort;
    }
    if (useHttp) {
        clientConfig.scheme = Aws::Http::Scheme::HTTP;
    }
    std::shared_ptr<Aws::S3::S3EndpointProviderBase> endpoints = Aws::MakeShared<Aws::S3::S3EndpointProvider>(Aws::S3::S3Client::ALLOCATION_TAG);
    _client = Aws::S3::S3Client (_cred, endpoints, clientConfig);

    // override service endpoint if provided
    if (!endpoint.empty()) {
        _client.OverrideEndpoint(Aws::String(endpoint.c_str()));
    }

    // create the bucket
    _bucketName = bucketName.c_str();
    Aws::S3::Model::CreateBucketRequest req;
    req.SetBucket(_bucketName);

    Aws::S3::Model::CreateBucketConfiguration cf;
    cf.SetLocationConstraint(Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(Aws::String(region.c_str())));
    
    req.SetCreateBucketConfiguration(cf);

    auto outcome = _client.CreateBucket(req);
    if (outcome.IsSuccess()) {
        //LOG(INFO) << "Bucket " << _bucketName << " created";
    } else if (outcome.GetError().GetExceptionName() == Aws::String("BucketAlreadyExists")) {
        //LOG(WARNING) << "Bucket " << _bucketName << " already exists";
    } else if (outcome.GetError().GetExceptionName() == Aws::String("BucketAlreadyOwnedByYou")) {
        //LOG(WARNING) << "Bucket " << _bucketName << " already owned";
    } else {
        LOG(ERROR) << "Failed to create bucket " << _bucketName << " in region " << region << " , " << outcome.GetError();
        exit(-1);
    }

    // set lifecycle management
    auto loutcome = _client.PutBucketLifecycleConfiguration(
        Aws::S3::Model::PutBucketLifecycleConfigurationRequest()
            .WithLifecycleConfiguration(
                Aws::S3::Model::BucketLifecycleConfiguration().WithRules(
                    { Aws::S3::Model::LifecycleRule()
                        .WithID("Remove non-current version chunks after 1 day")
                        .WithFilter(
                            Aws::S3::Model::LifecycleRuleFilter().WithPrefix("")
                        )
                        .WithNoncurrentVersionExpiration(
                            Aws::S3::Model::NoncurrentVersionExpiration()
                            .WithNoncurrentDays(1)
                        )
                        .WithStatus(Aws::S3::Model::ExpirationStatus::Enabled)
                    }
                )
            )
            .WithBucket(_bucketName)
    );

    if (!loutcome.IsSuccess() &&
        loutcome.GetError().GetExceptionName() != Aws::String("BucketAlreadyExists") &&
        loutcome.GetError().GetExceptionName() != Aws::String("BucketAlreadyOwnedByYou")
    )
        LOG(WARNING) << "Failed to enable lifecycles for bucket " << _bucketName << ", " << loutcome.GetError();
    
    // enable versioning
    Aws::S3::Model::PutBucketVersioningRequest vreq;
    vreq
        .WithVersioningConfiguration(
            Aws::S3::Model::VersioningConfiguration()
                .WithStatus(Aws::S3::Model::BucketVersioningStatus::Enabled))
        .WithBucket(_bucketName)
    ;
    auto voutcome = _client.PutBucketVersioning(vreq);

    if (!voutcome.IsSuccess() &&
        voutcome.GetError().GetExceptionName() != Aws::String("BucketAlreadyExists") &&
        voutcome.GetError().GetExceptionName() != Aws::String("BucketAlreadyOwnedByYou")
    )
        LOG(WARNING) << "Failed to enable versioning for bucket " << _bucketName << ", " << voutcome.GetError();

    updateUsage();
}

AwsContainer::~AwsContainer() {
}

bool AwsContainer::genObjectPath(char *opath, std::string chunkName) {
    return sprintf(opath, "%s", chunkName.c_str()) < OBJ_PATH_MAX;
}

bool AwsContainer::compareChecksum(const Aws::String &etag, const unsigned char *md5, const std::string &chunkName) {
    std::string md5Hex = ChecksumCalculator::toHex(md5, MD5_DIGEST_LENGTH);
    // skip the double quotes around the etag
    bool matched = false;
    try {
        matched = etag.substr(1, etag.size() - 2) == Aws::String(md5Hex.data(), md5Hex.size());
    } catch (std::exception &e) {
    }
    LOG_IF(ERROR, !matched) << "Chunk " << chunkName << " checksum mismatched (" << etag.substr(1, etag.size() - 2) << " vs " << md5Hex << ")";
    return matched;
}

bool AwsContainer::copyChecksum(const Aws::String &etag, unsigned char *md5) {
    if (md5 == NULL)
        return false;

    try {
        std::string hash = etag.substr(1, etag.size() - 2).c_str();
        ChecksumCalculator::unHex(hash, md5, MD5_DIGEST_LENGTH);
    } catch (std::exception &e) {
        return false;
    }

    return true;
}

bool AwsContainer::putChunk(Chunk &chunk) {
    std::string chunkName = chunk.getChunkName();

    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunkName) == false) {
        LOG(ERROR) << "Failed to generate object name";
        return false;
    }

    boost::timer::cpu_timer mytimer;

    // fill in the request template
    Aws::S3::Model::PutObjectRequest req;
    req.WithBucket(_bucketName).WithKey(opath);

    Aws::String dataString((char*) chunk.data, chunk.size);
    req.SetBody(
        Aws::MakeShared<Aws::StringStream>(
            "PutObjectInputStream",
            dataString
        )
    );

    // send the request
    auto outcome = _client.PutObject(req);

    double elapsed = mytimer.elapsed().wall * 1.0 / 1e9;

    bool success = outcome.IsSuccess();
    // verify checksum
    if (success && Config::getInstance().verifyChunkChecksum()) {
        success = compareChecksum(outcome.GetResult().GetETag(), chunk.md5, chunkName);
    }

    // check the response
    if (success) {
        LOG(INFO) << "Put chunk " << chunkName << " as object "
                  << opath << " with version " << outcome.GetResult().GetVersionId() 
                  << " (remote chunk access in " << elapsed << " s at speed " << (chunk.size * 1.0 / (1 << 20)) / elapsed << " MB/s)";
        // mark the current chunk version for chunk reverting (by deleting the current version)
        snprintf(chunk.chunkVersion, CHUNK_VERSION_MAX_LEN - 1, "%s", outcome.GetResult().GetVersionId().c_str()); 
        // copy checksum from response
        copyChecksum(outcome.GetResult().GetETag(), chunk.md5);
    } else {
        LOG(ERROR) << "Failed to put chunk " << chunkName << " as object " << opath;
    }

    return success;
}

bool AwsContainer::getChunk(Chunk &chunk, bool skipVerification) {
    std::string chunkName = chunk.getChunkName();

    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunkName) == false) {
        LOG(ERROR) << "Failed to generate object name";
        return false;
    }

    boost::timer::cpu_timer mytimer;

    // fill in the request template
    Aws::S3::Model::GetObjectRequest req;
    req.WithBucket(_bucketName).WithKey(opath);

    // send the request
    auto outcome = _client.GetObject(req);

    bool success = outcome.IsSuccess();
    double elapsed = mytimer.elapsed().wall * 1.0 / 1e9;

    // check the response
    if (success) {
        auto &dataStream = outcome.GetResult().GetBody();
        // get the chunk size
        dataStream.seekp(0, std::ios_base::end);
        chunk.size = dataStream.tellp();
        dataStream.seekp(0);
        // get the chunk data
        chunk.data = (unsigned char *) malloc (chunk.size);
        outcome.GetResult().GetBody().read((char *) chunk.data, chunk.size);
        // verify chunk checksum
        success = skipVerification || !Config::getInstance().verifyChunkChecksum() || chunk.verifyMD5();
    }
    if (!success) {
        LOG(ERROR) << "Failed to get chunk " << chunkName << " as object " << opath;
        return false;
    }

    LOG(INFO) << "Get chunk " << chunkName << " from path " << opath << " (remote chunk access in " << elapsed << " s at speed " << (chunk.size * 1.0 / (1 << 20)) / elapsed << " MB/s)";
    
    return true;
}

bool AwsContainer::deleteChunk(const Chunk &chunk) {
    std::string chunkName = chunk.getChunkName();

    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunkName) == false) {
        LOG(ERROR) << "Failed to generate object name";
        return false;
    }

    // fill in the request template
    Aws::S3::Model::DeleteObjectRequest req;
    req.WithBucket(_bucketName).WithKey(opath);

    // send the request
    _client.DeleteObject(req);

    return true;
}

bool AwsContainer::copyChunk(const Chunk &src, Chunk &dst) {
    char sopath[OBJ_PATH_MAX], dopath[OBJ_PATH_MAX];
    if (genObjectPath(sopath, src.getChunkName()) == false) {
        LOG(ERROR) << "Failed to generate object name";
        return false;
    }
    if (genObjectPath(dopath, dst.getChunkName()) == false) {
        LOG(ERROR) << "Failed to generate object name";
        return false;
    }

    boost::timer::cpu_timer mytimer;
    Aws::S3::Model::CopyObjectRequest req;
    req.WithBucket(_bucketName).WithKey(dopath).WithCopySource(_bucketName + "/" + sopath);

    // send the request
    auto outcome = _client.CopyObject(req);
    double elapsed = mytimer.elapsed().wall * 1.0 / 1e9;

    // check the response
    if (outcome.IsSuccess()) {
        LOG(INFO) << "Copy chunk " << sopath << " to chunk " << dopath << " (remote chunk access in " << elapsed << " s at speed " << (src.size * 1.0 / (1 << 20)) / elapsed << " MB/s)";
    } else {
        LOG(ERROR) << "Failed to copy chunk " << sopath << " to chunk " << dopath;
        return false;
    }

    Aws::S3::Model::HeadObjectRequest req2;
    req2.WithBucket(_bucketName).WithKey(dopath);

    auto outcome2 = _client.HeadObject(req2);

    bool success = outcome2.IsSuccess();

    if (success) {
        // copy resulted chunk size
        dst.size = outcome2.GetResult().GetContentLength();
        // copy checksum from response
        copyChecksum(outcome2.GetResult().GetETag(), dst.md5);
        // verify chunk checksum
        if (Config::getInstance().verifyChunkChecksum()) {
            success = compareChecksum(outcome2.GetResult().GetETag(), src.md5, dst.getChunkName());
        }
    }
    if (!success) {
        LOG(ERROR) << "Failed to get the size of chunk " << dopath << " after copying";
        deleteChunk(dst);
        return false;
    }

    return true;
}

bool AwsContainer::moveChunk(const Chunk &src, Chunk &dst) {
    // the size and md5 will be copied in copyChunk()
    return copyChunk(src, dst) && deleteChunk(src);
}

bool AwsContainer::hasChunk(const Chunk &chunk) {
    std::string chunkName = chunk.getChunkName();

    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunkName) == false) {
        LOG(ERROR) << "Failed to generate object name";
        return false;
    }

    // fill in the request template
    Aws::S3::Model::HeadObjectRequest req;
    req.WithBucket(_bucketName).WithKey(opath);

    auto outcome = _client.HeadObject(req);

    return outcome.IsSuccess() && // chunk existance
            outcome.GetResult().GetContentLength() == chunk.size && // chunk size
            (
                !Config::getInstance().verifyChunkChecksum() || // chunk checksum
                compareChecksum(outcome.GetResult().GetETag(), chunk.md5, chunkName)
            )
    ;
}

bool AwsContainer::revertChunk(const Chunk &chunk) {
    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to generate object name";
        return false;
    }

    // reference: https://docs.aws.amazon.com/en_pv/AmazonS3/latest/dev/RestoringPreviousVersions.html

    // fill in the request template
    Aws::S3::Model::DeleteObjectRequest req;
    req.WithBucket(_bucketName).WithKey(opath).WithVersionId(chunk.getChunkVersion());
    
    auto outcome = _client.DeleteObject(req);

    if (!outcome.IsSuccess())
        LOG(ERROR) << "Failed to revert chunk " << opath << " by removing version " << chunk.getChunkVersion() << ", " << outcome.GetError();

    return outcome.IsSuccess();
}

bool AwsContainer::verifyChunk(const Chunk &chunk) {
    std::string chunkName = chunk.getChunkName();

    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunkName) == false) {
        LOG(ERROR) << "Failed to generate object name";
        return false;
    }

    bool matched = false;

    Aws::S3::Model::HeadObjectRequest req;
    req.WithBucket(_bucketName).WithKey(opath);

    auto outcome = _client.HeadObject(req);

    matched = outcome.IsSuccess() && compareChecksum(outcome.GetResult().GetETag(), chunk.md5, chunkName);
    DLOG(INFO) << "Check chunk " << opath << " using HeadObj request, result = " << matched;

    return matched;
}

bool AwsContainer::getTotalSize(unsigned long int &total, bool needsLock) {
    total = 0;

    // get a list of all objects in the bucket
    Aws::S3::Model::ListObjectsRequest req;
    req.WithBucket(_bucketName);

    bool listAll = false;
    Aws::String nextMarker = "";
    while (!listAll) {
        req.WithMarker(nextMarker);
        auto outcome = _client.ListObjects(req);
            
        if (outcome.IsSuccess()) {
            // sum up the size of all objects
            Aws::Vector<Aws::S3::Model::Object> objList = outcome.GetResult().GetContents();
            for (auto const &obj: objList) {
                total += obj.GetSize();
            }
            // keep getting next page of objects if list is truncated (use last object key as the marker)
            listAll = !outcome.GetResult().GetIsTruncated();
            nextMarker = outcome.GetResult().GetNextMarker();
            if (!listAll && nextMarker.empty() && !objList.empty())
                nextMarker = objList.back().GetKey();
        } else {
            LOG(INFO) << "Failed to obtain a list of objects to calculate usage, " <<
                outcome.GetError().GetExceptionName() << " " <<
                outcome.GetError().GetMessage();

            return false;
        }
    }

    return true;
}

void AwsContainer::updateUsage() {
    unsigned long int total = 0;
    if (getTotalSize(total))
        _usage = total;
    else
        LOG(WARNING) << "Failed to update the size of container id = " << _id;
}
