using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Alexa.NET.Request;
using Amazon;
using Amazon.S3;
using Newtonsoft.Json.Linq;

namespace Alexa.NET.StateManagement.S3
{
    public class S3PersistenceStore:IPersistenceStore
    {
        public IAmazonS3 S3Client { get; } 
        public string BucketName { get; }

        public string KeyPrefix { get; set; }
        private ConcurrentDictionary<string,JObject> Local { get; } = new ConcurrentDictionary<string, JObject>();

        private bool BucketChecked = false;

        public S3PersistenceStore(string bucketName, IAmazonS3 s3Client)
        {
            if (string.IsNullOrWhiteSpace(bucketName))
            {
                throw new ArgumentNullException(nameof(bucketName));
            }

            BucketName = bucketName;
            S3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        }

        public async Task<T> Get<T>(SkillRequest request, string key)
        {
            await BucketCheck();
            var s3Key = GetS3Key(request);
            if (!Local.TryGetValue(key, out JObject values))
            {
                var newLocal = await GetFromS3(s3Key);
                Local.GetOrAdd(key, newLocal ?? new JObject());
            }

            return default(T);
        }

        private async Task BucketCheck()
        {
            if (BucketChecked)
            {
                return;
            }

            if (await S3Client.DoesS3BucketExistAsync(BucketName))
            {
                BucketChecked = true;
                return;
            }

            throw new InvalidOperationException($"Bucket {BucketName} does not exist");
        }

        private async Task<JObject> GetFromS3(string s3Key)
        {
            try
            {
                var response  = await S3Client.GetObjectAsync(BucketName, s3Key);
                if (response.HttpStatusCode == HttpStatusCode.OK)
                {

                }

                return null;
            }
            catch (AmazonS3Exception)
            {
                return null;
            }
        }

        protected string GetS3Key(SkillRequest request)
        {
            return request.Context.System.User.UserId;
        }

        public Task Set<T>(SkillRequest request, string key, T value)
        {
            throw new NotImplementedException();
        }

        public Task Save(SkillRequest request)
        {
            throw new NotImplementedException();
        }
    }
}
