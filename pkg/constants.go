package pkg

const (
	DBUser = "root" // The username used to authenticate with the database.
	DBPassword = "password" // The password for the specified username.
	DBHostname = "localhost" // The hostname or IP address of the Oracle database server.
	DBPort = "5432" // The port number where the Oracle listener is running.
	DBName = "testdb" // The service name or Pluggable Database (PDB) name. XEPDB1 is the default PDB name for Oracle XE (Express Edition).

	DBTable = "SFLW_RECS"

	WorkerCount = 2
)
