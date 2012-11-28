<?php
require_once(dirname(__FILE__) . '/config.php');

/**
 * PDO extension to use in daemons. Designed to support long-living connections
 * @author Aleksei Vesnin <dizeee@dizeee.ru>
 * @version 1.0
 * @license MIT
 */
class DaemonsDb
{
	/**
	 * @var PDO - A single instance of a connection object
	 */
	private static $db;

	/**
	 * Stop trying to reconnect after this number of retries in a row
	 */
	const MAX_CONNECTION_RETRIES = 10;

	/**
	 * @var int - Retries counter
	 */
	private static $connection_retries = 0;

	/**
	 * Uses a single PDO connection in every class instance
	 */
	public function __construct()
	{
		if (!self::$db)
		{
			self::reconnect();
		}
	}

	/**
	 * Executes a PDO method, called within this class instance
	 * @param $method_name
	 * @param $args
	 * @return mixed
	 */
	public function __call($method_name, $args)
	{
		if (method_exists($this->db(), $method_name)) ;
		return call_user_func_array(array($this->db(), $method_name), $args);
	}

	/**
	 * @return PDO
	 */
	public function db()
	{
		return self::$db;
	}

	/**
	 * Handles an error log
	 * @param string $str - Error message to put in log
	 */
	private static function errlog($str)
	{
		printf("[%s] ERROR: %s\n", date("C"), $str);
		// Better log to a file, uh?
	}

	/**
	 * Establish a new connection instead of a previous if one
	 * @return PDO
	 */
	public static function reconnect()
	{
		self::$db = null;

		self::$db = new PDO(DB_CONNECTION_STRING, DB_USER, DB_PASSWORD);
		self::$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		self::$db->query("set names '" . DB_CHARSET . "'");
	}

	/**
	 * Perform a query with a prepared statement
	 * @param string $sql
	 * @param array $params
	 * @return PDOStatement
	 */
	public function query($sql, $params = array())
	{
		try
		{
			$query = $this->db()->prepare($sql);
			$query->execute($params);
		}
		catch (Exception $e)
		{
			if (isset($query) && $query)
			{
				$err_info = $query->errorInfo();
				$mysql_err_code = $err_info[1];
				$mysql_err_msg = $err_info[2];

				// Handle lost connections
				if (in_array($mysql_err_code, array(2006, 2013)))
				{
					$err_str = 'Connection failed with error ' . $mysql_err_code . ' "' . $mysql_err_msg . '".';
					if (++self::$connection_retries >= self::MAX_CONNECTION_RETRIES)
					{
						self::errlog($err_str . ' Maximum retries made (' . self::MAX_CONNECTION_RETRIES . '). Exiting...');
						exit;
					}
					self::errlog($err_str . ' Trying to reconnect...');
					self::reconnect();
					return $this->query($sql, $params);
				}
			}
			self::errlog(
				"Query execution failed: " . print_r($query, true) . PHP_EOL .
					"Params: " . print_r($params, true) . PHP_EOL .
					"ErrorInfo: " . print_r($query->errorInfo(), true)
			);
			return false;
			//exit;
		}
		self::$connection_retries = 0;
		return $query;
	}

	/**
	 * Executes a query and returns all fetched results
	 * @param string $sql
	 * @param array $params
	 * @return array
	 */
	public function queryAll($sql, $params = array())
	{
		$query = $this->query($sql, $params);
		return $query->fetchAll(PDO::FETCH_ASSOC);
	}

	/**
	 * Executes a query and returns a single row of data.
	 * LIMIT 1 is added here so use a query without it
	 * @param string $sql
	 * @param array $params
	 * @return mixed
	 */
	public function queryRow($sql, $params = array())
	{
		$query = $this->query($sql . " LIMIT 1", $params);
		return $query->fetch();
	}

	/**
	 * Executes a query and returns one single value of first column in a first row found
	 * @param string $sql
	 * @param array $params
	 * @return mixed|null
	 */
	public function queryScalar($sql, $params = array())
	{
		$row = $this->queryRow($sql, $params);
		if ($row)
		{
			foreach ($row as $var) return $var;
		}
		return null;
	}

	/**
	 * Executes a query and returns an array of values from the first column
	 * @param string $sql
	 * @param array $params
	 * @return array
	 */
	public function queryColumn($sql, $params = array())
	{
		$query = $this->query($sql, $params);
		$col = array();
		while ($c = $query->fetchColumn())
			$col[] = $c;
		return $col;
	}

	/**
	 * Inserts a single row into a table.
	 * @param string $table
	 * @param array $data
	 * @return string|bool - Last insert id or false if a query failed to execute
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

		$query = $this->query($sql, $params);
		return $query ? $this->db()->lastInsertId() : false;
	}

	/**
	 * Updates a table
	 * @param string $table - A table name
	 * @param array $data
	 * @param string $where
	 * @param array $params
	 * @return PDOStatement
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

		return $this->query($sql, $params);
	}

	/**
	 * Deletes data from a table
	 * @param string $table
	 * @param string $where
	 * @param array $params
	 * @return PDOStatement
	 */
	public function delete($table, $where, $params = array())
	{
		$table = str_replace('`', '', $table);
		$sql = 'DELETE FROM `' . $table . '` WHERE ' . $where;
		return $this->query($sql, $params);

	}

}
