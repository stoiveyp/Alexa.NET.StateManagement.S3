using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Alexa.NET.Request;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Alexa.NET.StateManagement.S3.Tests
{
    public class StoreTests
    {
        [Fact]
        public void ConstructorThrowsOnNullBucket()
        {
            Assert.Throws<ArgumentNullException>(() => new S3PersistenceStore(null, Substitute.For<IAmazonS3>()));
        }

        [Fact]
        public void ConstructorThrowsOnEmptyBucket()
        {
            Assert.Throws<ArgumentNullException>(() => new S3PersistenceStore(string.Empty, Substitute.For<IAmazonS3>()));
        }

        [Fact]
        public void ConstructorThrowsOnNullS3Client()
        {
            Assert.Throws<ArgumentNullException>(() => new S3PersistenceStore("test", (IAmazonS3)null));
        }

        [Fact]
        public async Task CheckS3BucketFirstTime()
        {
            var mock = Substitute.For<IAmazonS3>();
            var bucketCheck = mock.DoesS3BucketExistAsync("xxx").Returns(true);

            var existenceCheck = mock
                .GetObjectAsync(string.Empty, string.Empty)
                .ReturnsForAnyArgs(c => new GetObjectResponse { HttpStatusCode = HttpStatusCode.NotFound });

            var request = DummyRequest();
            var s3 = new S3PersistenceStore("xxx", mock);
            var result = await s3.Get<string>(request, "test");
            Assert.Null(result);

            await mock.Received(1).DoesS3BucketExistAsync("xxx");
        }

        [Fact]
        public async Task InvalidBucketThrows()
        {
            var mock = Substitute.For<IAmazonS3>();
            var bucketCheck = mock.DoesS3BucketExistAsync("xxx").Returns(false);

            var request = DummyRequest();
            var s3 = new S3PersistenceStore("xxx", mock);
            await Assert.ThrowsAsync<InvalidOperationException>(() => s3.Get<string>(request, "test"));
        }

        [Fact]
        public async Task GetCheckS3CorrectKey()
        {
            var mock = Substitute.For<IAmazonS3>();
            var bucketCheck = mock.DoesS3BucketExistAsync("xxx").Returns(true);

            var existenceCheck = mock
                .GetObjectAsync(string.Empty, string.Empty)
                .ReturnsForAnyArgs(c => new GetObjectResponse { HttpStatusCode = HttpStatusCode.NotFound });

            var request = DummyRequest();
            var s3 = new S3PersistenceStore("xxx", mock);
            var result = await s3.Get<string>(request, "test");
            Assert.Null(result);

            await mock.Received(1).GetObjectAsync("xxx", "abc123");
        }

        [Fact]
        public async Task SetCheckS3CorrectKey()
        {
            var mock = Substitute.For<IAmazonS3>();
            var bucketCheck = mock.DoesS3BucketExistAsync("xxx").Returns(true);

            var existenceCheck = mock
                .GetObjectAsync(string.Empty, string.Empty)
                .ReturnsForAnyArgs(c => new GetObjectResponse { HttpStatusCode = HttpStatusCode.NotFound });

            var request = DummyRequest();
            var s3 = new S3PersistenceStore("xxx", mock);
            await s3.Set(request, "test", "wibble");

            await mock.Received(1).GetObjectAsync("xxx", "abc123");
        }

        [Fact]
        public async Task RemoveWorksWithNoKeys()
        {
            var mock = Substitute.For<IAmazonS3>();
            mock.DoesS3BucketExistAsync("xxx").Returns(true);
            mock
                .GetObjectAsync(string.Empty, string.Empty)
                .ReturnsForAnyArgs(c => new GetObjectResponse { HttpStatusCode = HttpStatusCode.NotFound });

            var store = new S3PersistenceStore("xxx",mock);
            await store.Set(DummyRequest(), "test", "value");
            var result1 = store.Remove(DummyRequest());
            var result2 = store.Remove(DummyRequest());

            Assert.True(result1);
            Assert.True(result2);
        }

        [Fact]
        public async Task SetTwiceDoesSingleCheck()
        {
            var mock = Substitute.For<IAmazonS3>();
            mock.DoesS3BucketExistAsync("xxx").Returns(true);

            mock
                .GetObjectAsync(string.Empty, string.Empty)
                .ReturnsForAnyArgs(c => new GetObjectResponse { HttpStatusCode = HttpStatusCode.NotFound });

            var request = DummyRequest();
            var s3 = new S3PersistenceStore("xxx", mock);
            await s3.Set(request, "test", "wibble");
            await s3.Set(request, "test2", "wibble2");

            await mock.Received(1).DoesS3BucketExistAsync("xxx");
            await mock.Received(1).GetObjectAsync("xxx", "abc123");
        }

        [Fact]
        public async Task SetPutsValueLocally()
        {
            var mock = Substitute.For<IAmazonS3>();
            mock.DoesS3BucketExistAsync("xxx").Returns(true);

            mock
                .GetObjectAsync(string.Empty, string.Empty)
                .ReturnsForAnyArgs(c => new GetObjectResponse { HttpStatusCode = HttpStatusCode.NotFound });

            var request = DummyRequest();
            var s3 = new S3PersistenceStore("xxx", mock);
            await s3.Set(request, "test", "wibble");

            var result = await s3.Get<string>(request, "test");
            Assert.Equal("wibble", result);
        }

        [Fact]
        public async Task SavePushesToS3()
        {
            var mock = Substitute.For<IAmazonS3>();
            mock.DoesS3BucketExistAsync("xxx").Returns(true);

            mock
                .GetObjectAsync(string.Empty, string.Empty)
                .ReturnsForAnyArgs(c => new GetObjectResponse { HttpStatusCode = HttpStatusCode.NotFound });

            var request = DummyRequest();
            var s3 = new S3PersistenceStore("xxx", mock);
            await s3.Set(request, "test", "wibble");

            mock.When(a => a.PutObjectAsync(Arg.Any<PutObjectRequest>())).Do(call =>
            {
                var put = call.Arg<PutObjectRequest>();
                Assert.Equal("xxx", put.BucketName);
                Assert.Equal("abc123", put.Key);
                Assert.NotNull(put.InputStream);
                using (var reader = new StreamReader(put.InputStream))
                {
                    var content = reader.ReadToEnd();
                    var data = JsonConvert.DeserializeObject<JObject>(content);
                    Assert.Equal("wibble", data.Value<string>("test"));
                }
            });

            await s3.Save(request);
        }

        [Fact]
        public async Task GetRetrievesS3()
        {
            var mock = Substitute.For<IAmazonS3>();
            mock.DoesS3BucketExistAsync("xxx").Returns(true);

            var oms = new MemoryStream();
            var writer = new StreamWriter(oms);

            var dictionary = new Dictionary<string, object> {{"test", "wibble"}};

            writer.Write(JsonConvert.SerializeObject(dictionary));
            writer.Flush();
            oms.Seek(0, SeekOrigin.Begin);

            mock
                .GetObjectAsync(string.Empty, string.Empty)
                .ReturnsForAnyArgs(c => new GetObjectResponse
                {
                    HttpStatusCode = HttpStatusCode.OK,
                    ResponseStream = oms
                });

            var request = DummyRequest();
            var s3 = new S3PersistenceStore("xxx", mock);
            var result = await s3.Get<string>(request,"test");
            Assert.Equal("wibble", result);
        }

        private SkillRequest DummyRequest()
        {
            return new SkillRequest
            { Context = new Context { System = new AlexaSystem { User = new User { UserId = "abc123" } } } };
        }
    }
}
