<?php

/*
 * Phalcon Cassandra
 * Copyright (c) 2016 David Hübner
 * This source file is subject to the New BSD License
 * Licence is bundled with this package in the file docs/LICENSE.txt
 * Author: David Hübner <david.hubner@gmail.com>
 */

namespace PhalconCassandra\Db\Adapter;

use PhalconCassandra\Db\Exception\Cassandra as CException,
    Phalcon\Db\Adapter,
    Phalcon\Db\AdapterInterface,
    Phalcon\Db\ResultInterface,
    Phalcon\Events\ManagerInterface,
    Cassandra\Cluster\Builder,
    Cassandra\SimpleStatement,
    Cassandra\BatchStatement,
    Cassandra\ExecutionOptions,
    Cassandra\Exception as BaseException;

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
     *  
     * @author  David Hübner <david.hubner at google.com>
     * @param   array $descriptor - connection description
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function __construct($descriptor)
    {
        $this->_transactionLevel = 0;
        $this->_type = 'cassandra';
        $this->_dialectType = 'cassandra';
        $descriptor['dialectClass'] = 'PhalconCassandra\\Db\\Dialect\\Cassandra';
        parent::__construct($descriptor);
    }

    /**
     * Establishes connection to existing cluster
     *  
     * @author  David Hübner <david.hubner at google.com>
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
     *  
     * @author  David Hübner <david.hubner at google.com>
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
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  \Cassandra\Session | null
     */
    public function getInternalHandler()
    {
        return $this->_session;
    }

    /**
     * Sends SQL statements to the database server returning the success state.
     * Use this method only when the SQL statement sent to the server is returning rows
     *
     * @author  David Hübner <david.hubner at google.com>
     * @param   string $cqlStatement - CQL statement
     * @param   array $bindParams - bind parameters, default null
     * @param   array $bindTypes - bind types, default null
     * @return  \Phalcon\Db\ResultInterface | bool
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function query($cqlStatement, $bindParams = null, $bindTypes = null)
    {
        $statement = $this->_prepareStatement($cqlStatement, $bindParams, $bindTypes);

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

        $this->_consistency = null;

        var_dump($result->first());
    }

    /**
     * Sends SQL statements to the database server returning the success state.
     * Use this method only when the SQL statement sent to the server doesn't return any rows
     * 
     * @author  David Hübner <david.hubner at google.com>
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
     * Internally executes CQL statement
     * 
     * @author  David Hübner <david.hubner at google.com>
     * @param   string $cqlStatement - CQL statement
     * @param   array $bindParams - bind parameters
     * @param   array $bindTypes - bind types
     * @return  \Cassandra\SimpleStatement
     */
    protected function _prepareStatement($cqlStatement, $bindParams, $bindTypes)
    {
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
     * 
     * @author  David Hübner <david.hubner at google.com>
     * @return  int
     */
    public function getConsistency()
    {
        return (is_null($this->_consistency) ? $this->_defaultConsistency : $this->_consistency);
    }

    /**
     * Sets consistency level for next statement execution
     * 
     * @author  David Hübner <david.hubner at google.com>
     * @return  \PhalconCassandra\Db\Adapter\Cassandra
     */
    public function setConsistency($consistency)
    {
        $this->_consistency = $consistency;
        return $this;
    }

    /**
     * Gets default consistency level
     * 
     * @author  David Hübner <david.hubner at google.com>
     * @return  int
     */
    public function getDefaultConsistency()
    {
        return $this->_defaultConsistency;
    }

    /**
     * Sets default consistency level
     * 
     * @author  David Hübner <david.hubner at google.com>
     * @return  \PhalconCassandra\Db\Adapter\Cassandra
     */
    public function setDefaultConsistency($consistency)
    {
        $this->_defaultConsistency = $consistency;
        return $this;
    }

    /**
     * Returns an array of Column objects describing a table
     *
     * @author  David Hübner <david.hubner at google.com>
     * @param   string $table
     * @param   string $schema
     * @return  \Phalcon\Db\ColumnInterface[] 
     */
    public function describeColumns($table, $schema = null)
    {
        throw new CException('Not supported');
    }

    /**
     * Escapes a column/table/schema name
     *
     * @author  David Hübner <david.hubner at google.com>
     * @param   string $identifier
     * @return  string
     */
    public function escapeIdentifier($identifier)
    {
        return $identifier;
    }

    /**
     * Escapes a string value
     *
     * @author  David Hübner <david.hubner at google.com>
     * @param   string $str
     * @return  string
     */
    public function escapeString($str)
    {
        return addslashes($str);
    }

    /**
     * Returns 0 if last executed query failed or 1 if was successfull
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  int
     */
    public function affectedRows()
    {
        return $this->_affectedRows;
    }

    /**
     * Not supported
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function lastInsertId($sequenceName = null)
    {
        return false;
    }

    /**
     * Starts new batch
     *
     * @author  David Hübner <david.hubner at google.com>
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
     *
     * @author  David Hübner <david.hubner at google.com>
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
     *
     * @author  David Hübner <david.hubner at google.com>
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
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function isUnderTransaction()
    {
        return ($this->_transactionLevel ? true : false);
    }

}
