<?php

/*
 * Phalcon Cassandra
 * Copyright (c) 2016 David Hübner
 * This source file is subject to the New BSD License
 * Licence is bundled with this package in the file docs/LICENSE.txt
 * Author: David Hübner <david.hubner@gmail.com>
 */

namespace PhalconCassandra\Db\Adapter;

use PhalconCassandra\Db\Adapter\Cassandra\Exception,
    Phalcon\Db\Adapter,
    Phalcon\Db\AdapterInterface,
    Phalcon\Db\ResultInterface,
    Cassandra\Cluster\Builder,
    Cassandra\Exception as ExceptionInterface;

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
     * Creates new Cassandra adapter
     *  
     * @author  David Hübner <david.hubner at google.com>
     * @param   array $descriptor - connection description
     * @throws  \PhalconCassandra\Db\Adapter\Cassandra\Exception
     */
    public function __construct($descriptor)
    {
        $descriptor['dialectClass'] = 'PhalconCassandra\\Db\\Dialect\\Cassandra';
        parent::__construct($descriptor);
    }

    /**
     * Establishes connection to existing cluster
     *  
     * @author  David Hübner <david.hubner at google.com>
     * @param   array $descriptor - connection description, default null
     * @return  bool   
     * @throws  \PhalconCassandra\Db\Adapter\Cassandra\Exception
     */
    public function connect($descriptor = null)
    {
        if ($this->_session) {
            throw new Exception('Connection already established');
        }

        if (is_array($descriptor)) {
            $descriptor = array_merge($this->_descriptor, $descriptor);
        } else {
            $descriptor = $this->_descriptor;
        }

        if (empty($descriptor['keyspace'])) {
            throw new Exception('Keyspace must be set');
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
        } catch (ExceptionInterface $e) {
            throw new Exception($e->getMessage(), $e->getCode());
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
     * Returns current connection session handler
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
     */
    public function query($cqlStatement, $bindParams = null, $bindTypes = null)
    {
        if ($this->_eventsManager) {
            $result = $this->_eventsManager->fire(
                'db:beforeQuery', $this, $bindParams
            );
            if ($result === false) {
                return false;
            }
        }
    }

    public function execute($sqlStatement, $placeholders = null, $dataTypes = null)
    {
        
    }

    public function describeColumns($table, $schema = null)
    {
        
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
     * Not supported
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function affectedRows()
    {
        return false;
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
     * Not supported, will be implemented as BATCH
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function begin($nesting = true)
    {
        return false;
    }

    /**
     * Not supported, will be implemented as BATCH
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function commit($nesting = true)
    {
        return false;
    }

    /**
     * Not supported, will be implemented as BATCH
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function rollback($nesting = true)
    {
        return false;
    }

    /**
     * Not supported, will be implemented as BATCH
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function isUnderTransaction()
    {
        return false;
    }

}
