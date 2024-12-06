package pkg

const (
	DBUser = "system" // The username used to authenticate with the database.
	DBPassword = "password" // The password for the specified username.
	DBHostname = "localhost" // The hostname or IP address of the Oracle database server.
	DBPort = "1521" // The port number where the Oracle listener is running.
	DBName = "XEPDB1" // The service name or Pluggable Database (PDB) name. XEPDB1 is the default PDB name for Oracle XE (Express Edition).

	DBTable = "SFLW_RECS"

	WorkerCount = 2
)
