using Dynamicweb.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Dynamicweb.DataIntegration.Providers.ODataProvider.Model;

public static class RetryHelper
{
    public static async Task RetryOnExceptionAsync(int maxRetryAttempts, Func<Task> operation, ILogger _logger)
    {
        await RetryOnExceptionAsync<Exception>(maxRetryAttempts, operation, _logger);
    }

    public static async Task RetryOnExceptionAsync<TException>(int maxRetryAttempts, Func<Task> operation, ILogger _logger) where TException : Exception
    {
        if (maxRetryAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRetryAttempts));
        }
        var retryattempts = 0;
        do
        {
            try
            {
                retryattempts++;
                await operation();
                break;
            }
            catch (TException ex)
            {
                if (retryattempts == maxRetryAttempts)
                {
                    _logger?.Error($"After {maxRetryAttempts} attempts, and no response", ex);
                }
                int delay = IncreasingDelayInSeconds(retryattempts);
                _logger?.Log($"Attempt {retryattempts} of {maxRetryAttempts} failed. New retry after {delay} seconds.");
                await CreateRetryDelayForException(maxRetryAttempts, retryattempts, delay);
            }
        } while (true);
    }

    private static Task CreateRetryDelayForException(int maxRetryAttempts, int attempts, int delay)
    {
        return Task.Delay(delay);
    }

    internal static int[] DelayPerAttemptInSeconds =
    {
        (int) TimeSpan.FromSeconds(5).TotalSeconds,
        (int) TimeSpan.FromSeconds(15).TotalSeconds,
        (int) TimeSpan.FromSeconds(30).TotalSeconds,
        (int) TimeSpan.FromSeconds(45).TotalSeconds,
        (int) TimeSpan.FromMinutes(1).TotalSeconds,
        (int) TimeSpan.FromMinutes(3).TotalSeconds,
        (int) TimeSpan.FromMinutes(5).TotalSeconds,
        (int) TimeSpan.FromMinutes(10).TotalSeconds,
        (int) TimeSpan.FromMinutes(15).TotalSeconds,
        (int) TimeSpan.FromMinutes(30).TotalSeconds
    };

    static int IncreasingDelayInSeconds(int failedAttempts)
    {
        if (failedAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException();
        }
        return failedAttempts >= DelayPerAttemptInSeconds.Length ? DelayPerAttemptInSeconds.Last() : DelayPerAttemptInSeconds[failedAttempts];
    }
}
