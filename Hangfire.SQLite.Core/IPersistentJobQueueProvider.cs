namespace Hangfire.SQLite.Core
{
    public interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue();
        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi();
    }
}
