using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Alexa.NET.Request;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;

namespace Alexa.NET.StateManagement.S3
{
    public class S3PersistenceStore : IPersistenceStore
    {
        public IAmazonS3 S3Client { get; }
        public string BucketName { get; }

        public string KeyPrefix { get; set; }
        private ConcurrentDictionary<string, Dictionary<string, object>> Local { get; } = new ConcurrentDictionary<string, Dictionary<string, object>>();

        private JsonSerializer Serializer { get; }

        private bool BucketChecked;

        public S3PersistenceStore(string bucketName, IAmazonS3 s3Client, JsonSerializerSettings jsonSettings = null)
        {
            if (string.IsNullOrWhiteSpace(bucketName))
            {
                throw new ArgumentNullException(nameof(bucketName));
            }

            BucketName = bucketName;
            S3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
            Serializer = jsonSettings == null ? JsonSerializer.CreateDefault() : JsonSerializer.Create(jsonSettings);
        }

        public async Task<T> Get<T>(SkillRequest request, string key)
        {
            var s3Key = GetS3Key(request);
            var values = await EnsureValues(s3Key);

            return values.ContainsKey(key) && values[key] is T ? (T)values[key] : default(T);
        }

        private async Task<Dictionary<string, object>> EnsureValues(string s3Key)
        {
            await BucketCheck();
            if (!Local.TryGetValue(s3Key, out Dictionary<string, object> values))
            {
                var newLocal = await GetFromS3(s3Key);
                values = Local.GetOrAdd(s3Key, newLocal ?? new Dictionary<string, object>());
            }

            return values;
        }

        public async Task Set<T>(SkillRequest request, string key, T value)
        {
            var s3Key = GetS3Key(request);
            var values = await EnsureValues(s3Key);
            if (values.ContainsKey(key))
            {
                values[key] = value;
            }
            else
            {
                values.Add(key, value);
            }
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

        private async Task<Dictionary<string, object>> GetFromS3(string s3Key)
        {
            try
            {
                var response = await S3Client.GetObjectAsync(BucketName, s3Key);
                if (response.HttpStatusCode == HttpStatusCode.OK)
                {
                    using (var reader = new JsonTextReader(new StreamReader(response.ResponseStream)))
                    {
                        return Serializer.Deserialize<Dictionary<string, object>>(reader);
                    }
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

        public async Task Save(SkillRequest request)
        {
            var s3Key = GetS3Key(request);
            if (Local.ContainsKey(s3Key))
            {
                var oms = new MemoryStream();
                var writer = new StreamWriter(oms);
                Serializer.Serialize(writer, Local[s3Key]);
                writer.Flush();


                oms.Seek(0, SeekOrigin.Begin);

                var putRequest = new PutObjectRequest
                {
                    BucketName = BucketName,
                    Key = s3Key,
                    InputStream = oms
                };
                await S3Client.PutObjectAsync(putRequest);
            }
        }
    }
}
