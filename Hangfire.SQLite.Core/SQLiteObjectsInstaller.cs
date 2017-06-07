﻿// This file is part of Hangfire.
// Copyright © 2013-2014 Sergey Odinokov.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Reflection;
using Dapper;
using Hangfire.Logging;
using Microsoft.Data.Sqlite;

namespace Hangfire.SQLite.Core
{
    internal static class SQLiteObjectsInstaller
    {
        private const int RequiredSchemaVersion = 5;
        private const int RetryAttempts = 3;        

        private static readonly ILog Log = LogProvider.GetLogger(typeof(SQLiteStorage));

        public static void Install(SqliteConnection connection)
        {
            Install(connection, null);
        }

        public static void Install(SqliteConnection connection, string schema)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            
            Log.Info("Start installing Hangfire SQL objects...");

            var script = GetStringResource(
                typeof(SQLiteObjectsInstaller).GetTypeInfo().Assembly, 
                "Hangfire.SQLite.Core.Install.sql");

            script = script.Replace("SET @TARGET_SCHEMA_VERSION = 5;", "SET @TARGET_SCHEMA_VERSION = " + RequiredSchemaVersion + ";");

            script = script.Replace("$(HangFireSchema)", !string.IsNullOrWhiteSpace(schema) ? schema : Constants.DefaultSchema);

            for (var i = 0; i < RetryAttempts; )
            {
                connection.Execute(script);
                break;
            }

            Log.Info("Hangfire SQL objects installed.");
        }

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null) 
                {
                    throw new InvalidOperationException(String.Format(
                        "Requested resource `{0}` was not found in the assembly `{1}`.",
                        resourceName,
                        assembly));
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
