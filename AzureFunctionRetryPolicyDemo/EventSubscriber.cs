using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Polly;

namespace AzureFunctionRetryPolicyDemo
{
    /// <summary>
    /// Service bus topic trigger azure function
    /// </summary>
    public class EventSubscriber
    {
        private const int _maxRetryAttempts = 10;
        private const int _oneSecondInMilliSeconds = 1000;
        private const int _maxDelayInMilliseconds = 32 * _oneSecondInMilliSeconds;
        private readonly Random _jitterer;
        private readonly ILogger<EventSubscriber> _logger;

        public EventSubscriber(ILogger<EventSubscriber> log)
        {
            _logger = log;
            _jitterer = new Random();

        }

        /// <summary>
        /// Service bus topic trigger azure function
        /// </summary>
        /// <param name="sbMsg"></param>
        /// <returns></returns>
        [FunctionName(nameof(EventSubscriber))]
        public async Task RunAsync([ServiceBusTrigger("ServiceBusConfigOption:Topic", "ServiceBusConfigOption:TopicSubscription",
            Connection = "ServiceBusConfigOption:ConnectionString")]string sbMsg)
        {
            _logger.LogInformation($"C# ServiceBus topic trigger function processed message: {sbMsg}");

            //Policy wrap to combine multiple policies
            var retryPolicy = Policy.WrapAsync(GetExponentialBackoffRetryPolicy(), GetThrottlingAwareRetryPolicy());

            await retryPolicy.ExecuteAsync(async () => await RunCoreAsync(sbMsg));
        }

        private async Task RunCoreAsync(string sbMsg)
        {
            try
            {
                _logger.LogInformation("{class} - {method} - Start",
                   nameof(EventSubscriber), nameof(EventSubscriber.RunAsync));

                //throwing cosmos exception
                throw new CosmosException("too many requests", HttpStatusCode.TooManyRequests, 429, "too many requests", 1);

                _logger.LogInformation("{class} - {method} - End",
                   nameof(EventSubscriber), nameof(EventSubscriber.RunAsync));

            }
            catch (Exception ex)
            {
                _logger.LogError($"Unable to process event  with {nameof(EventSubscriber)}," +
                   $" ExceptionMessage:{ex.Message}, StackTrace: {ex.StackTrace}");
                throw;
            }
        }

        /// <summary>
        /// Exponential back off retry policy
        /// </summary>
        /// <returns>The IAsyncPolicy instance.</returns>
        /// <remarks></remarks>
        private IAsyncPolicy GetExponentialBackoffRetryPolicy()
        {
            var policy = Policy
            .Handle<CosmosException>(ex => ex.StatusCode != HttpStatusCode.TooManyRequests)
            .WaitAndRetryAsync(
                _maxRetryAttempts,
                retryAttempt =>
                {
                    var calculatedDelayInMilliseconds = Math.Pow(2, retryAttempt) * _oneSecondInMilliSeconds;

                    _logger.LogInformation($"calculatedDelayInMilliseconds:{calculatedDelayInMilliseconds}");

                    var jitterInMilliseconds = _jitterer.Next(0, _oneSecondInMilliSeconds);

                    _logger.LogInformation($"jitterInMilliseconds:{jitterInMilliseconds}");

                    var actualDelay = Math.Min(calculatedDelayInMilliseconds + jitterInMilliseconds, _maxDelayInMilliseconds);

                    _logger.LogInformation($"actualDelay:{actualDelay}");

                    return TimeSpan.FromMilliseconds(actualDelay);
                }
                );
            return policy;
        }

        /// <summary>
        /// Throttling aware retry policy
        /// </summary>
        /// <returns>The IAsyncPolicy instance.</returns>
        /// <remarks></remarks>
        private IAsyncPolicy GetThrottlingAwareRetryPolicy()
        {
            var policy = Policy
                .Handle<CosmosException>(ex => ex.StatusCode == HttpStatusCode.TooManyRequests)
                .WaitAndRetryAsync(_maxRetryAttempts,
                    sleepDurationProvider: (_, ex, __) => ((CosmosException)ex).RetryAfter.Value,
                    onRetryAsync: (_, __, ___, ____) => Task.CompletedTask);
            return policy;
        }

    }
}
