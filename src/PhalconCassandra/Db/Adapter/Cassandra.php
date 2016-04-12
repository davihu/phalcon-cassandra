<?php

/*
 * Phalcon Cassandra
 * Copyright (c) 2016 David Hübner
 * This source file is subject to the New BSD License
 * Licence is bundled with this package in the file docs/LICENSE.txt
 * Author: David Hübner <david.hubner@gmail.com>
 */

namespace PhalconCassandra\Db\Adapter;

use PhalconCassandra\Db\Exception\Cassandra as CException;
use PhalconCassandra\Db\Column\Cassandra as CColumn;
use PhalconCassandra\Db\Result\Cassandra as CResult;
use Phalcon\Db\Adapter;
use Phalcon\Db\AdapterInterface;
use Phalcon\Events\ManagerInterface;
use Cassandra\Cluster\Builder;
use Cassandra\SimpleStatement;
use Cassandra\BatchStatement;
use Cassandra\Table as BaseTable;
use Cassandra\Column as BaseColumn;
use Cassandra\Type as BaseType;
use Cassandra\ExecutionOptions;
use Cassandra\Exception as BaseException;

/**
 * Cassandra DB adapter for Phalcon
 *
 * @author     David Hübner <david.hubner at google.com>
 * @version    Release: @package_version@
 * @since      Release 1.0
 */
class Cassandra extends Adapter implements AdapterInterface
{

    /**
     * @var \Cassandra\Session $_session - cassandra connection session
     */
    protected $_session;

    /**
     * @var array $_schemaCache - cached cassandra schemas
     */
    protected $_schemaCache;

    /**
     * @var \Cassandra\BatchStatement $_batch - transaction batch
     */
    protected $_batch;

    /**
     * @var int $_defaultConsistency - default consistency level
     */
    protected $_defaultConsistency = \Cassandra::CONSISTENCY_LOCAL_ONE;

    /**
     * @var int $_consistency - next execute or query consistency level
     */
    protected $_consistency;

    /**
     * @var int $_affectedRows - last affected rows
     */
    protected $_affectedRows;

    /**
     * Creates new Cassandra adapter
     * @param array $descriptor - connection description
     */
    public function __construct($descriptor)
    {
        $this->_transactionLevel = 0;
        $this->_type = 'cassandra';
        $this->_dialectType = 'cassandra';
        $this->_schemaCache = array();
        if (empty($descriptor['dialectClass'])) {
            $descriptor['dialectClass'] = 'PhalconCassandra\\Db\\Dialect\\Cassandra';
        }
        parent::__construct($descriptor);
    }

    /**
     * Establishes connection to existing cluster
     * @param   array $descriptor - connection description, default null
     * @return  bool
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function connect($descriptor = null)
    {
        if ($this->_session) {
            throw new CException('Connection already established');
        }

        if (is_array($descriptor)) {
            $descriptor = array_merge($this->_descriptor, $descriptor);
        } else {
            $descriptor = $this->_descriptor;
        }

        if (empty($descriptor['keyspace'])) {
            throw new CException('Keyspace must be set');
        }

        $cluster = new Builder();

        if (isset($descriptor['host'])) {
            $cluster->withContactPoints($descriptor['host']);
        }

        if (isset($descriptor['port'])) {
            $cluster->withPort($descriptor['port']);
        }

        if (isset($descriptor['consistency'])) {
            $cluster->withDefaultConsistency($descriptor['consistency']);
            $this->setDefaultConsistency($descriptor['consistency']);
        }

        if (isset($descriptor['pageSize'])) {
            $cluster->withDefaultPageSize($descriptor['pageSize']);
        }

        if (isset($descriptor['persistent'])) {
            $cluster->withPersistentSessions($descriptor['persistent']);
        }

        if (isset($descriptor['persistent'])) {
            $cluster->withPersistentSessions($descriptor['persistent']);
        }

        if (isset($descriptor['keepalive'])) {
            $cluster->withTCPKeepalive($descriptor['keepalive']);
        }

        if (isset($descriptor['connectTimeout'])) {
            $cluster->withConnectTimeout($descriptor['connectTimeout']);
        }

        if (isset($descriptor['requestTimeout'])) {
            $cluster->withRequestTimeout($descriptor['requestTimeout']);
        }

        if (isset($descriptor['ioThreads'])) {
            $cluster->withIOThreads($descriptor['ioThreads']);
        }

        if (isset($descriptor['username'], $descriptor['password'])) {
            $cluster->withCredentials($descriptor['username'], $descriptor['password']);
        }

        try {
            $this->_session = $cluster->build()->connect($descriptor['keyspace']);
        } catch (BaseException $e) {
            throw new CException($e->getMessage(), $e->getCode());
        }

        return true;
    }

    /**
     * Closes actual connection session to the cluster
     * @return  bool
     */
    public function close()
    {
        if ($this->_session) {
            $this->_session->close();
            $this->_session = null;
        }
        return true;
    }

    /**
     * Returns current Cassandra session handler
     * @return  \Cassandra\Session | null
     */
    public function getInternalHandler()
    {
        return $this->_session;
    }

    /**
     * Sends SQL statements to the database server returning the success state.
     * Use this method only when the SQL statement sent to the server is returning rows
     * @param   string $cqlStatement - CQL statement
     * @param   array $bindParams - bind parameters, default null
     * @param   array $bindTypes - bind types, default null
     * @return  \Phalcon\Db\ResultInterface | bool
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function query($cqlStatement, $bindParams = null, $bindTypes = null)
    {
        $statement = $this->_prepareStatement($cqlStatement, $bindParams, $bindTypes);

        // @todo pager
        if (isset($bindParams['APL0'])) {
            unset($bindParams['APL0']);
        }

        $params = [
            'consistency' => $this->getConsistency()
        ];

        if ($bindParams) {
            $params['arguments'] = $bindParams;
        }

        try {
            $result = $this->_session->execute($statement, new ExecutionOptions($params));
        } catch (BaseException $e) {
            throw new CException($e->getMessage(), $e->getCode());
        }

        if ($this->_eventsManager instanceof ManagerInterface) {
            $this->_eventsManager->fire('db:afterQuery', $this, $bindParams);
        }

        $finalResult = new CResult(
            $this, $result, $cqlStatement, $bindParams, $bindTypes, $this->getConsistency()
        );

        $this->_consistency = null;

        return $finalResult;
    }

    /**
     * Sends SQL statements to the database server returning the success state.
     * Use this method only when the SQL statement sent to the server doesn't return any rows
     * @param   string $cqlStatement - CQL statement
     * @param   array $bindParams - bind parameters, default null
     * @param   array $bindTypes - bind types, default null
     * @return  bool
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function execute($cqlStatement, $bindParams = null, $bindTypes = null)
    {
        $statement = $this->_prepareStatement($cqlStatement, $bindParams, $bindTypes);

        if ($this->_transactionLevel) {
            $this->_batch->add($statement, $bindParams);
        } else {
            $params = [
                'consistency' => $this->getConsistency()
            ];

            if ($bindParams) {
                $params['arguments'] = $bindParams;
            }

            try {
                $this->_session->execute($statement, new ExecutionOptions($params));
            } catch (BaseException $e) {
                throw new CException($e->getMessage(), $e->getCode());
            }
        }

        $this->_affectedRows = 1;

        if ($this->_eventsManager instanceof ManagerInterface) {
            $this->_eventsManager->fire('db:afterQuery', $this, $bindParams);
        }

        $this->_consistency = null;
        return true;
    }

    /**
     * Prepares statement
     * @param   string $cqlStatement - CQL statement
     * @param   array $bindParams - bind parameters
     * @param   array $bindTypes - bind types
     * @return  \Cassandra\SimpleStatement
     */
    protected function _prepareStatement($cqlStatement, $bindParams, $bindTypes)
    {
        $cqlStatement = preg_replace('!(\w+\.)(\w+)!', '\\2', $cqlStatement);

        if ($this->_eventsManager instanceof ManagerInterface) {
            $this->_sqlStatement = $cqlStatement;
            $this->_sqlVariables = $bindParams;
            $this->_sqlBindTypes = $bindTypes;
            $result = $this->_eventsManager->fire(
                'db:beforeQuery', $this, $bindParams
            );
            if ($result === false) {
                return false;
            }
        }

        $this->_affectedRows = 0;
        return new SimpleStatement($cqlStatement);
    }

    /**
     * Gets consistency level for next statement execution
     * @return  int
     */
    public function getConsistency()
    {
        return (is_null($this->_consistency) ? $this->_defaultConsistency : $this->_consistency);
    }

    /**
     * Sets consistency level for next statement execution
     * @return  \PhalconCassandra\Db\Adapter\Cassandra
     */
    public function setConsistency($consistency)
    {
        $this->_consistency = $consistency;
        return $this;
    }

    /**
     * Gets default consistency level
     * @return  int
     */
    public function getDefaultConsistency()
    {
        return $this->_defaultConsistency;
    }

    /**
     * Sets default consistency level
     * @return  \PhalconCassandra\Db\Adapter\Cassandra
     */
    public function setDefaultConsistency($consistency)
    {
        $this->_defaultConsistency = $consistency;
        return $this;
    }

    /**
     * Escapes a column/table/schema name
     * @param   string $identifier
     * @return  string
     */
    public function escapeIdentifier($identifier)
    {
        return $identifier;
    }

    /**
     * Escapes a string value
     * @param   string $str
     * @return  string
     */
    public function escapeString($str)
    {
        return addslashes($str);
    }

    /**
     * Returns 0 if last executed query failed or 1 if was successfull
     * @return  int
     */
    public function affectedRows()
    {
        return $this->_affectedRows;
    }

    /**
     * Last insert id is not supported by Cassandra
     * @return  bool
     */
    public function lastInsertId($sequenceName = null)
    {
        return false;
    }

    /**
     * Starts new batch
     * @param   int $batchType - default \Cassandra::BATCH_LOGGED
     * @return  bool
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function begin($batchType = \Cassandra::BATCH_LOGGED)
    {
        if (empty($this->_session)) {
            return false;
        }

        if ($this->_transactionLevel) {
            throw new CException('Nested transactions not supported');
        }

        if ($this->_eventsManager instanceof ManagerInterface) {
            $this->_eventsManager->fire('db:beginTransaction', $this);
        }

        $this->_transactionLevel = 1;
        $this->_batch = new BatchStatement($type);
        return true;
    }

    /**
     * Executes active batch
     * @return  bool
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function commit($nesting = false)
    {
        if (empty($this->_session)) {
            return false;
        }

        if (empty($this->_transactionLevel)) {
            throw new CException('There is no active batch');
        }

        if ($this->_eventsManager instanceof ManagerInterface) {
            $this->_eventsManager->fire('db:db:commitTransaction', $this);
        }

        $params = [
            'consistency' => $this->getConsistency()
        ];

        try {
            $this->_session->execute($this->_batch, new ExecutionOptions($params));
        } catch (BaseException $e) {
            throw new CException($e->getMessage(), $e->getCore());
        }

        $this->_transactionLevel = 0;
        $this->_batch = null;
        $this->_consistency = null;
        return true;
    }

    /**
     * Discards active batch
     * @return  bool
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function rollback($nesting = false)
    {
        if (empty($this->_session)) {
            return false;
        }

        if (empty($this->_transactionLevel)) {
            throw new CException('There is no active batch');
        }

        if ($this->_eventsManager instanceof ManagerInterface) {
            $this->_eventsManager->fire('db:rollbackTransaction', $this);
        }

        $this->_transactionLevel = 0;
        $this->_batch = null;
        return true;
    }

    /**
     * Checks whether the connection is under a transaction
     * @return  bool
     */
    public function isUnderTransaction()
    {
        return ($this->_transactionLevel ? true : false);
    }

    /**
     * Checks if table exists in given keyspace
     * @param   string $tableName - table name
     * @param   string $keyspaceName - keyspace name, default null
     * @return  bool
     */
    public function tableExists($tableName, $keyspaceName = null)
    {
        if (empty($keyspaceName)) {
            $keyspaceName = $this->_descriptor['keyspace'];
        }

        $keyspace = $this->_session->schema()->keyspace($keyspaceName);

        if (empty($keyspace)) {
            return false;
        }

        $table = $keyspace->table($tableName);

        return ($table instanceof BaseTable ? true : false);
    }

    /**
     * Returns an array of Column objects describing a table
     * @param   string $tableName
     * @param   string $keyspaceName
     * @return  \Phalcon\Db\ColumnInterface[]
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function describeColumns($tableName, $keyspaceName = null)
    {
        if (empty($keyspaceName)) {
            $keyspaceName = $this->_descriptor['keyspace'];
        }

        $keyspace = $this->_session->schema()->keyspace($keyspaceName);

        if (empty($keyspace)) {
            throw new CException('Keyspace "' . $keyspaceName . '" not exists');
        }

        $table = $keyspace->table($tableName);

        if (empty($table)) {
            throw new CException('Table "' . $tableName . '" not exists');
        }

        $columns = $table->columns();

        $result = [];
        $prevCol = null;

        foreach ($columns as $col) {
            $result[] = $this->_describeColumn($col, $prevCol, $tableName, $keyspaceName);
            $prevCol = $col;
        }

        return $result;
    }

    /**
     * Describes one cassandra column
     * @param   \Cassandra\Column $col
     * @param   \Cassandra\Column | null $prevCol
     * @param   string $tableName
     * @param   string $keyspaceName
     * @return  \Phalcon\Db\ColumnInterface
     */
    protected function _describeColumn(BaseColumn $col, $prevCol, $tableName, $keyspaceName)
    {
        $name = $col->name();

        $def = [
            'reversed' => $col->isReversed(),
            'static' => $col->isStatic(),
            'frozen' => $col->isFrozen()
        ];

        list($def['primary'], $def['partitionKey'], $def['clusteringKey']) = $this->_getColumnKeyData($name, $tableName, $keyspaceName);
        list($def['type'], $def['bindType'], $def['isNumeric']) = $this->_getColumnTypeData($col->type());

        if ($prevCol instanceof BaseColumn) {
            $def['after'] = $prevCol->name();
        } else {
            $def['first'] = true;
        }

        return new CColumn($name, $def);
    }

    /**
     * Gets column key data, reads data from system.schema_columns
     * Need to have read access to this system table
     * @param   string $columnName
     * @param   string $tableName
     * @param   string $keyspaceName
     * @return  array
     */
    protected function _getColumnKeyData($columnName, $tableName, $keyspaceName)
    {
        $statement = new SimpleStatement(
            'SELECT type FROM system.schema_columns WHERE keyspace_name = ? AND columnfamily_name = ? AND column_name = ?'
        );

        $params = [
            'consistency' => \Cassandra::CONSISTENCY_LOCAL_ONE,
            'arguments' => [$keyspaceName, $tableName, $columnName]
        ];

        $result = $this->_session->execute($statement, new ExecutionOptions($params));

        if ($result[0]['type'] == 'partition_key') {
            return [true, true, false];
        }

        if ($result[0]['type'] == 'clustering_key') {
            return [true, true, true];
        }

        return [false, false, false];
    }

    /**
     * Gets column type data
     * @param   \Cassandra\Type $type
     * @return  array
     */
    protected function _getColumnTypeData(BaseType $type)
    {
        switch ($type->name()) {
            case 'int':
                return [CColumn::TYPE_INTEGER, CColumn::BIND_PARAM_INT, true];
            case 'varchar':
                return [CColumn::TYPE_VARCHAR, CColumn::BIND_PARAM_STR, false];
            case 'text':
                return [CColumn::TYPE_TEXT, CColumn::BIND_PARAM_STR, false];
            case 'timestamp':
                return [CColumn::TYPE_TIMESTAMP, CColumn::BIND_PARAM_INT, true];
            case 'boolean':
                return [CColumn::TYPE_BOOLEAN, CColumn::BIND_PARAM_BOOL, false];
            case 'decimal':
                return [CColumn::TYPE_DECIMAL, CColumn::BIND_PARAM_DECIMAL, true];
            case 'double':
                return [CColumn::TYPE_DOUBLE, CColumn::BIND_PARAM_DECIMAL, true];
            case 'uuid':
                return [CColumn::TYPE_UUID, CColumn::BIND_PARAM_UUID, false];
            case 'timeuuid':
                return [CColumn::TYPE_TIMEUUID, CColumn::BIND_PARAM_UUID, false];
            case 'ascii':
                return [CColumn::TYPE_ASCII, CColumn::BIND_PARAM_STR, false];
            case 'bigint':
                return [CColumn::TYPE_BIGINTEGER, CColumn::BIND_PARAM_INT, true];
            case 'blob':
                return [CColumn::TYPE_BLOB, CColumn::BIND_PARAM_BLOB, false];
            case 'counter':
                return [CColumn::TYPE_COUNTER, CColumn::BIND_PARAM_INT, true];
            case 'float':
                return [CColumn::TYPE_FLOAT, CColumn::BIND_PARAM_DECIMAL, true];
            case 'inet':
                return [CColumn::TYPE_INET, CColumn::BIND_PARAM_STR, false];
            case 'list':
                return [CColumn::TYPE_LIST, CColumn::BIND_PARAM_ARRAY, false];
            case 'map':
                return [CColumn::TYPE_MAP, CColumn::BIND_PARAM_ARRAY, false];
            case 'set':
                return [CColumn::TYPE_SET, CColumn::BIND_PARAM_ARRAY, false];
            case 'varint':
                return [CColumn::TYPE_VARINT, CColumn::BIND_PARAM_STR, false];
            default:
                throw new CException('Unsupported data type ' . $type->name());
        }
    }
}
