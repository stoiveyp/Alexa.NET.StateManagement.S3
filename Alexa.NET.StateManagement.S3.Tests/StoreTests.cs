using System;
using System.Net;
using System.Threading.Tasks;
using Alexa.NET.Request;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using NSubstitute;
using Xunit;

namespace Alexa.NET.StateManagement.S3.Tests
{
    public class StoreTests
    {
        [Fact]
        public void ConstructorThrowsOnNullBucket()
        {
            Assert.Throws<ArgumentNullException>(() => new S3PersistenceStore(null,Substitute.For<IAmazonS3>()));
        }

        [Fact]
        public void ConstructorThrowsOnEmptyBucket()
        {
            Assert.Throws<ArgumentNullException>(() => new S3PersistenceStore(string.Empty,Substitute.For<IAmazonS3>()));
        }

        [Fact]
        public void ConstructorThrowsOnNullS3Client()
        {
            Assert.Throws<ArgumentNullException>(() => new S3PersistenceStore("test",(IAmazonS3)null));
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
            var s3 = new S3PersistenceStore("xxx",mock);
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
        public async Task CheckS3CorrectKey()
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

            await mock.Received(1).GetObjectAsync("xxx","abc123");
        }



        private SkillRequest DummyRequest()
        {
            return new SkillRequest
                { Context = new Context { System = new AlexaSystem { User = new User { UserId = "abc123" } } } };
        }
    }
}
