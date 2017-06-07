using System;

namespace Hangfire.SQLite.Core
{
    public class SQLiteDistributedLockException : Exception
    {
        public SQLiteDistributedLockException(string message)
            : base(message)
        {
        }
    }
}
