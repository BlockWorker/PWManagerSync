using System.Data.SQLite;

namespace PWManagerSync {
    public static class Database {
        private const string IDENT_TABLE_SCHEMA = @"CREATE TABLE idents (
            user TEXT NOT NULL,
            ident TEXT NOT NULL,
            iter INTEGER NOT NULL DEFAULT 0,
            symbols TEXT NOT NULL DEFAULT ' ',
            longpw INTEGER NOT NULL DEFAULT 1,
            timestamp TEXT NOT NULL,
            PRIMARY KEY (user, ident) ON CONFLICT REPLACE
        )";

        private const string IDENT_STAGING_TABLE_SCHEMA = @"CREATE TABLE ident_staging (
            uuid TEXT NOT NULL,
            user TEXT NOT NULL,
            ident TEXT NOT NULL,
            iter INTEGER NOT NULL DEFAULT 0,
            symbols TEXT NOT NULL DEFAULT ' ',
            longpw INTEGER NOT NULL DEFAULT 1,
            timestamp TEXT NOT NULL,
            del INTEGER NOT NULL,
            PRIMARY KEY (uuid, user, ident) ON CONFLICT REPLACE
        )";

        private const string APP_TABLE_SCHEMA = @"CREATE TABLE apps (
            user TEXT NOT NULL,
            pkg TEXT NOT NULL,
            ident TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            PRIMARY KEY (user, pkg) ON CONFLICT REPLACE
        )";

        private const string APP_STAGING_TABLE_SCHEMA = @"CREATE TABLE app_staging (
            uuid TEXT NOT NULL,
            user TEXT NOT NULL,
            pkg TEXT NOT NULL,
            ident TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            del INTEGER NOT NULL,
            PRIMARY KEY (uuid, user, pkg) ON CONFLICT REPLACE
        )";

        private const string SYNC_TIMES_SCHEMA = @"CREATE TABLE sync_times (
            user TEXT NOT NULL PRIMARY KEY ON CONFLICT REPLACE,
            lastsync TEXT NOT NULL
        )";

        private static SQLiteConnection? conn;

        public static async Task<bool> Init() {
            try {
                if (File.Exists("pwmsync.db")) { //existing DB
                    conn = new("Data Source=pwmsync.db;Version=3;");
                    await conn.OpenAsync();

                    await ClearStaged();

                    Console.WriteLine("Loaded existing DB.");
                } else { //new DB
                    conn = new("Data Source=pwmsync.db;Version=3;New=True;");
                    await conn.OpenAsync();

                    Console.WriteLine("Creating new DB...");

                    using (SQLiteTransaction tx = conn.BeginTransaction()) {
                        using (SQLiteCommand cmd = conn.CreateCommand()) {
                            cmd.CommandText = IDENT_TABLE_SCHEMA;
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = IDENT_STAGING_TABLE_SCHEMA;
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = APP_TABLE_SCHEMA;
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = APP_STAGING_TABLE_SCHEMA;
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = SYNC_TIMES_SCHEMA;
                            cmd.ExecuteNonQuery();
                        }
                        await tx.CommitAsync();
                    }
                }

                return true;
            } catch (Exception e) {
                Console.WriteLine(e.ToString());
                return false;
            }
        }

        public static void Close() {
            conn?.Close();
        }

        public static async Task<List<IdentEntity>> GetAllIdents(string user) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = "SELECT ident, iter, symbols, longpw, timestamp FROM idents WHERE user=@User";
                cmd.Parameters.AddWithValue("User", user);

                var reader = await cmd.ExecuteReaderAsync();

                var list = new List<IdentEntity>();

                while (await reader.ReadAsync()) {
                    var entity = new IdentEntity() {
                        Identifier = reader.GetString(0),
                        Iteration = unchecked((uint)reader.GetInt32(1)),
                        Symbols = reader.GetString(2),
                        LongPW = reader.GetBoolean(3),
                        Timestamp = DateTime.Parse(reader.GetString(4), null, System.Globalization.DateTimeStyles.AdjustToUniversal) //ensure time is in UTC
                    };
                    list.Add(entity);
                }

                return list;
            }
        }

        public static async Task<List<AppEntity>> GetAllApps(string user) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = "SELECT pkg, ident, timestamp FROM apps WHERE user=@User";
                cmd.Parameters.AddWithValue("User", user);

                var reader = await cmd.ExecuteReaderAsync();

                var list = new List<AppEntity>();

                while (await reader.ReadAsync()) {
                    var entity = new AppEntity() {
                        Package = reader.GetString(0),
                        Identifier = reader.GetString(1),
                        Timestamp = DateTime.Parse(reader.GetString(2), null, System.Globalization.DateTimeStyles.AdjustToUniversal) //ensure time is in UTC
                    };
                    list.Add(entity);
                }

                return list;
            }
        }

        public static async Task StageInsertIdents(Guid uuid, string user, IEnumerable<IdentEntity> idents) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteTransaction trans = conn.BeginTransaction()) {
                using (SQLiteCommand cmd = conn.CreateCommand()) {
                    cmd.CommandText = "INSERT OR REPLACE INTO ident_staging VALUES (@Uuid, @User, @Ident, @Iter, @Symbols, @Longpw, @Timestamp, 0)"; //del=0: this is an insertion/update

                    var identParam = new SQLiteParameter("Ident");
                    var iterParam = new SQLiteParameter("Iter");
                    var symbolsParam = new SQLiteParameter("Symbols");
                    var longpwParam = new SQLiteParameter("Longpw");
                    var timestampParam = new SQLiteParameter("Timestamp");

                    cmd.Parameters.AddRange(new SQLiteParameter[] { identParam, iterParam, symbolsParam, longpwParam, timestampParam });
                    cmd.Parameters.AddWithValue("Uuid", uuid.ToString());
                    cmd.Parameters.AddWithValue("User", user);

                    foreach (var entity in idents) {
                        identParam.Value = entity.Identifier;
                        iterParam.Value = unchecked((int)entity.Iteration);
                        symbolsParam.Value = entity.Symbols;
                        longpwParam.Value = entity.LongPW;
                        timestampParam.Value = entity.Timestamp.ToString("O"); //store time in ISO format

                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                await trans.CommitAsync();
            }
        }

        public static async Task StageInsertApps(Guid uuid, string user, IEnumerable<AppEntity> apps) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteTransaction trans = conn.BeginTransaction()) {
                using (SQLiteCommand cmd = conn.CreateCommand()) {
                    cmd.CommandText = "INSERT OR REPLACE INTO app_staging VALUES (@Uuid, @User, @Package, @Ident, @Timestamp, 0)"; //del=0: this is an insertion/update

                    var packageParam = new SQLiteParameter("Package");
                    var identParam = new SQLiteParameter("Ident");
                    var timestampParam = new SQLiteParameter("Timestamp");

                    cmd.Parameters.AddRange(new SQLiteParameter[] { packageParam, identParam, timestampParam });
                    cmd.Parameters.AddWithValue("Uuid", uuid.ToString());
                    cmd.Parameters.AddWithValue("User", user);

                    foreach (var entity in apps) {
                        packageParam.Value = entity.Package;
                        identParam.Value = entity.Identifier;
                        timestampParam.Value = entity.Timestamp.ToString("O"); //store time in ISO format

                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                await trans.CommitAsync();
            }
        }

        public static async Task StageDeleteIdents(Guid uuid, string user, IEnumerable<string> idents) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteTransaction trans = conn.BeginTransaction()) {
                using (SQLiteCommand cmd = conn.CreateCommand()) {
                    cmd.CommandText = "INSERT OR REPLACE INTO ident_staging VALUES (@Uuid, @User, @Ident, 0, '', 0, '', 1)"; //del=1: this is a deletion

                    var identParam = new SQLiteParameter("Ident");

                    cmd.Parameters.Add(identParam);
                    cmd.Parameters.AddWithValue("Uuid", uuid.ToString());
                    cmd.Parameters.AddWithValue("User", user);

                    foreach (var ident in idents) {
                        identParam.Value = ident;

                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                await trans.CommitAsync();
            }
        }

        public static async Task StageDeleteApps(Guid uuid, string user, IEnumerable<string> packages) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteTransaction trans = conn.BeginTransaction()) {
                using (SQLiteCommand cmd = conn.CreateCommand()) {
                    cmd.CommandText = "INSERT OR REPLACE INTO app_staging VALUES (@Uuid, @User, @Package, '', '', 1)"; //del=1: this is a deletion

                    var packageParam = new SQLiteParameter("Package");

                    cmd.Parameters.Add(packageParam);
                    cmd.Parameters.AddWithValue("Uuid", uuid.ToString());
                    cmd.Parameters.AddWithValue("User", user);

                    foreach (var package in packages) {
                        packageParam.Value = package;

                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                await trans.CommitAsync();
            }
        }

        public static async Task CommitStaged(Guid uuid) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = //intuition: commit staged updates and deletions to corresponding tables, and delete them from the staging tables
                    "INSERT OR REPLACE INTO idents SELECT user, ident, iter, symbols, longpw, timestamp FROM ident_staging WHERE uuid=@Uuid AND del=0; " +
                    "DELETE FROM idents WHERE (user, ident) IN (SELECT user, ident FROM ident_staging WHERE uuid=@Uuid AND del=1); " +
                    "DELETE FROM ident_staging WHERE uuid=@Uuid; " +
                    "INSERT OR REPLACE INTO apps SELECT user, pkg, ident, timestamp FROM app_staging WHERE uuid=@Uuid AND del=0; " +
                    "DELETE FROM apps WHERE (user, pkg) IN (SELECT user, pkg FROM app_staging WHERE uuid=@Uuid AND del=1); " +
                    "DELETE FROM app_staging WHERE uuid=@Uuid";

                cmd.Parameters.AddWithValue("Uuid", uuid.ToString());

                await cmd.ExecuteNonQueryAsync();
            }
        }

        public static async Task Unstage(Guid uuid) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteCommand cmd = conn.CreateCommand()) {
                cmd.CommandText =
                    "DELETE FROM ident_staging WHERE uuid=@Uuid; " +
                    "DELETE FROM app_staging WHERE uuid=@Uuid";

                cmd.Parameters.AddWithValue("Uuid", uuid.ToString());

                await cmd.ExecuteNonQueryAsync();
            }
        }

        public static async Task ClearStaged() {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = "DELETE FROM ident_staging; DELETE FROM app_staging";
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public static async Task<DateTime?> GetLastSync(string user) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = "SELECT lastsync FROM sync_times WHERE user=@User";
                cmd.Parameters.AddWithValue("User", user);

                var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync()) return DateTime.Parse(reader.GetString(0), null, System.Globalization.DateTimeStyles.AdjustToUniversal); //ensure time is in UTC
                else return null;
            }
        }

        public static async Task SetLastSync(string user, DateTime lastSync) {
            if (conn == null) throw new InvalidOperationException("Database must be initialised before accessing it.");

            using (SQLiteCommand cmd = conn.CreateCommand()) {
                cmd.CommandText = "INSERT OR REPLACE INTO sync_times VALUES (@User, @Lastsync)";

                cmd.Parameters.AddWithValue("User", user);
                cmd.Parameters.AddWithValue("Lastsync", lastSync.ToString("O")); //store time in ISO format

                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}
