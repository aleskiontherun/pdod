<?php

/**
 * PDO extension to use in daemons. Designed to support long-living MySQL connections.
 *
 * Here is a small usage example:
 *
 * <code>
 *
 * <?php
 *
 * require_once('PDOd.php');
 *
 * $db = new PDOd('mysql:host=127.0.0.1;dbname=test', 'root', 'mypasswd', 'utf8');
 *
 * $pid = posix_getpid();
 *
 * while (true):
 *     if ($db->update('tasks', array('worker' => $pid), 'worker=0 LIMIT 1')):
 *         $task = $db->queryRow('SELECT * FROM tasks WHERE worker = :pid', array(':pid' => $pid));
 *         // Do some work
 *     else:
 *         sleep(1);
 *     endif;
 * endwhile;
 *
 * // We never get here
 *
 * </code>
 *
 * All native PDO methods are bound to this class through the __call method to provide
 * a consistent errors handling by the class and respawning lost connections.
 *
 * @author Aleksei Vesnin <dizeee@dizeee.ru>
 * @version 2.0.1
 * @license MIT
 * @link https://github.com/dizeee/pdod
 * @link http://php.net/manual/en/class.pdo.php
 *
 * @method PDOStatement prepare(string $statement, array $driver_options = null) Prepares a statement for execution and returns a statement object
 * @method bool beginTransaction() Initiates a transaction
 * @method bool commit() Commits a transaction
 * @method bool rollBack() Rolls back a transaction
 * @method bool inTransaction() Checks if inside a transaction
 * @method bool setAttribute(integer $attribute, mixed $value) Set an attribute
 * @method bool exec(string $statement) Execute an SQL statement and return the number of affected rows
 * @method string lastInsertId(string $name = null) Returns the ID of the last inserted row or sequence value
 * @method mixed errorCode() Fetch the SQLSTATE associated with the last operation on the database handle
 * @method array errorInfo() Fetch extended error information associated with the last operation on the database handle
 * @method mixed getAttribute(integer $attribute) Retrieve a database connection attribute
 * @method string quote(string $string, integer $parameter_type = PDO::PARAM_STR) Quotes a string for use in a query.
 */
class PDOd
{
	/**
	 * Stop trying to reconnect after this number of retries in a row
	 */
	const MAX_CONNECTION_RETRIES = 10;

	/**
	 * @var integer - Retries counter
	 */
	public $connection_retries = 0;

	protected $connectionString;
	protected $username;
	protected $password;
	protected $charset;

	/**
	 * @var PDO - An instance of a connection object
	 */
	protected $pdo;

	/**
	 * Init the connection
	 */
	public function __construct($connection_string = "", $username = "", $password = "", $charset = "utf8")
	{
		$this->connectionString = $connection_string;
		$this->username = $username;
		$this->password = $password;
		$this->charset = $charset;

		$this->reconnect();
	}

	/**
	 * Executes a PDO method, called within this class instance
	 * @param $method_name
	 * @param $args
	 * @return mixed
	 */
	public function __call($method_name, $args)
	{
		if (method_exists($this->pdo, $method_name))
		{
			try
			{
				return call_user_func_array(array($this->pdo, $method_name), $args);
			}
			catch (PDOException $e)
			{
				return $this->onException($e, $method_name, $args);
			}
		}
		return false;
	}

	/**
	 * Handles an error log
	 * @param string $str The error message to log
	 */
	private static function errlog($str)
	{
		$str = sprintf("[%s] ERROR: %s\n", date("c"), $str);
		echo $str;
		// Better log to a file, uh?
	}

	/**
	 * Handles PDO Exceptions. Tries to reconnect on one of the following MySQL errors:
	 * #2006 MySQL Server has gone away
	 * #2013 Lost connection to MySQL server during query
	 * Exits with an error message if failed to reconnect {@link PDOd::MAX_CONNECTION_RETRIES} times
	 * @param PDOException $e The exception instance
	 * @param string $method A PDOd or PDO method name to be executed after a connection is restored
	 * @param array $arguments The method arguments
	 * @return mixed|bool The method result if it is a lost connection exception or false otherwise
	 */
	public function onException(PDOException $e, $method = '', $arguments = array())
	{

		// Handle lost connections
		if (in_array($e->errorInfo[1], array(2006, 2013)))
		{
			$err_str = 'Connection failed with error ' . $e->errorInfo[1] . ' "' . $e->errorInfo[2] . '".';
			if (++$this->connection_retries >= self::MAX_CONNECTION_RETRIES)
			{
				self::errlog($err_str . ' Maximum retries made (' . self::MAX_CONNECTION_RETRIES . '). Exiting...');
				exit(1);
			}
			// Uncomment the following to log lost connections
			//self::errlog($err_str . ' Trying to reconnect...');
			$this->reconnect();
			if ($method)
			{
				$result = call_user_func_array(array($this, $method), $arguments);
				$this->connection_retries = 0;
				return $result;
			}
		}
		// And log all other exceptions with a stack trace
		else
		{
			$msg = 'Query execution failed with error ' . $e->getCode() . ':' . PHP_EOL . $e->getMessage() . PHP_EOL;
			$msg .= 'Stack trace:' . PHP_EOL;
			$msg .= $e->getTraceAsString() . PHP_EOL;
			if ($arguments)
			{
				$msg .= 'Arguments: ' . print_r($arguments, true) . PHP_EOL;
			}
			self::errlog($msg);
		}
		return false;
	}

	/**
	 * Establish a new connection instead of a previous if one
	 * @return PDO
	 */
	public function reconnect()
	{
		$this->pdo = null;

		try
		{
			$this->pdo = new PDO($this->connectionString, $this->username, $this->password);
			$this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

			if ($this->charset)
			{
				$this->query("SET NAMES :charset", array(':charset' => $this->charset));
			}
		}
		catch (PDOException $e)
		{
			$this->onException($e);
			exit(1);
		}
	}

	/**
	 * Perform a query with a prepared statement
	 * @param string $sql The SQL statement to be executed
	 * @param array $params Parameters to be bound to the query
	 * @return PDOStatement|bool The executed query instance or false on failure
	 */
	public function query($sql, $params = array())
	{
		try
		{
			$query = $this->pdo->prepare($sql);
			$query->execute($params);
		}
		catch (PDOException $e)
		{
			return $this->onException($e, __FUNCTION__, func_get_args());
		}
		return $query;
	}

	/**
	 * Executes a query and returns all fetched results
	 * @param string $sql The SQL statement to be executed
	 * @param array $params Parameters to be bound to the query
	 * @return array|bool Fetched data or false on failure
	 */
	public function queryAll($sql, $params = array())
	{
		if ($query = $this->query($sql, $params))
		{
			return $query->fetchAll(PDO::FETCH_ASSOC);
		}
		return false;
	}

	/**
	 * Executes a query and returns a single row of data in an array.
	 * @param string $sql The SQL statement to be executed
	 * @param array $params Parameters to be bound to the query
	 * @return array|bool Fetched data or false on failure
	 */
	public function queryRow($sql, $params = array())
	{
		if ($query = $this->query($sql, $params))
		{
			return $query->fetch(PDO::FETCH_ASSOC);
		}
		return false;
	}

	/**
	 * Executes a query and returns an array of values from a column
	 * @param string $sql The SQL statement to be executed
	 * @param array $params Parameters to be bound to the query
	 * @param integer $column_number Zero-indexed number of the column to retrieve from the row
	 * @return array|bool Fetched data or false on failure
	 */
	public function queryColumn($sql, $params = array(), $column_number = 0)
	{
		if ($query = $this->query($sql, $params))
		{
			return $query->fetchAll(PDO::FETCH_COLUMN, $column_number);
		}
		return false;
	}

	/**
	 * Executes a query and returns one single value of first column in a first row found
	 * @param string $sql The SQL statement to be executed
	 * @param array $params Parameters to be bound to the query
	 * @return mixed|bool The value or false on failure
	 */
	public function queryScalar($sql, $params = array())
	{
		if ($query = $this->query($sql, $params))
		{
			return $query->fetchColumn();
		}
		return false;
	}

	/**
	 * Inserts a single row into a table.
	 * @param string $table The table name to insert data into
	 * @param array $data Data to be inserted (column name => column value)
	 * @return string|bool The ID of the inserted row or false on failure
	 */
	public function insert($table, $data)
	{
		$params = array();
		$keys = array();
		foreach ($data as $key => $val)
		{
			$params[':' . $key] = $val;
			$keys[] = $key;
		}
		$sql = "INSERT INTO " . $table . " (`" . implode('`, `', $keys) . "`) VALUES (:" . implode(', :', $keys) . ")";

		if ($query = $this->query($sql, $params))
		{
			return $this->pdo->lastInsertId();
		}
		return false;
	}

	/**
	 * Updates a table
	 * @param string $table The table name to update data in
	 * @param array $data Data to be updated (column name => column value)
	 * @param string $where SQL expression for WHERE clause
	 * @param array $params Parameters to be bound to the WHERE clause
	 * @return integer|bool The number of updated rows or false on failure
	 */
	public function update($table, $data, $where = '1', $params = array())
	{
		$sql = 'UPDATE `' . $table . '` SET ';
		$set = array();
		foreach ($data as $key => $val)
		{
			$params[':' . $key] = $val;
			$set[] = '`' . $key . '`=:' . $key;
		}
		$sql .= implode(', ', $set);
		$sql .= " WHERE " . $where;

		if ($query = $this->query($sql, $params))
		{
			return $query->rowCount();
		}
		return false;
	}

	/**
	 * Deletes data from a table
	 * @param string $table The table name to delete data from
	 * @param string $where SQL expression for WHERE clause
	 * @param array $params Parameters to be bound to the WHERE clause
	 * @return integer|bool The number of deleted rows or false on failure
	 */
	public function delete($table, $where, $params = array())
	{
		$sql = 'DELETE FROM `' . $table . '` WHERE ' . $where;
		if ($query = $this->query($sql, $params))
		{
			return $query->rowCount();
		}
		return false;
	}

}
