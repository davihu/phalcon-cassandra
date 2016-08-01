<?php

/*
 * Phalcon Cassandra
 * Copyright (c) 2016 David Hübner
 * This source file is subject to the New BSD License
 * Licence is bundled with this package in the file docs/LICENSE.txt
 * Author: David Hübner <david.hubner@gmail.com>
 */

namespace PhalconCassandra\Db\Result;

use PhalconCassandra\Db\Exception\Cassandra as CException;
use Phalcon\Db;
use Phalcon\Db\AdapterInterface;
use Phalcon\Db\ResultInterface;
use Cassandra\Rows as BaseResult;

/**
 * Cassandra result
 *
 * @author     David Hübner <david.hubner at google.com>
 * @version    Release: @package_version@
 * @since      Release 1.0
 */
class Cassandra implements ResultInterface
{

    /**
     * @var \Phalcon\Db\AdapterInterface $_connection - Cassandra DB adapter
     */
    protected $_connection;

    /**
     * @var \Cassandra\Rows $_internalResult - Cassandra result
     */
    protected $_internalResult;

    /**
     *
     * @var string $_cqlStatement - CQL statement
     */
    protected $_clqStatement;

    /**
     * @var array $_bindParameters - bind parameters
     */
    protected $_bindParams;

    /**
     * @var array $_bindTypes - bind types
     */
    protected $_bindTypes;

    /**
     * @var int $_consistency - consistency level
     */
    protected $_consistency;

    /**
     * @var int $_numRows - number of result rows
     */
    protected $_numRows;

    /**
     * @var int $_seekIndex - index of seek result
     */
    protected $_seekIndex = 0;

    /**
     * @var int $_fetchMode - fetch mode, default associative array
     */
    protected $_fetchMode = Db::FETCH_ASSOC;

    /**
     * Cassandra result constructor
     *
     * @param \Phalcon\Db\AdapterInterface $connection - Cassandra DB adapter
     * @param \Cassandra\Rows $result - Cassandra result
     * @param string $cqlStatement - CQL statement
     * @param array $bindParams - bind parameters, default null
     * @param array $bindTypes - bind types, default null
     * @param int $consistency - consistency level, default null
     */
    public function __construct(AdapterInterface $connection, BaseResult $result, $cqlStatement = null, $bindParams = null, $bindTypes = null, $consistency = null)
    {
        $this->_connection = $connection;
        $this->_internalResult = $result;
        $this->_clqStatement = $cqlStatement;
        $this->_bindParams = $bindParams;
        $this->_bindTypes = $bindTypes;
        $this->_consistency = $consistency;
        $this->_numRows = count($result);
    }

    /**
     * Allows to executes the statement again. Some database systems don't support scrollable cursors,
     * So, as cursors are forward only, we need to execute the cursor again to fetch rows from the begining
     * @return mixed
     */
    public function execute()
    {
        $this->_connection->setConsistency($this->_consistency);
        return $this->_connection->query($this->_clqStatement, $this->_bindParams, $this->_bindTypes);
    }

    /**
     * Fetches an array/object of strings that corresponds to the fetched row,
     * or FALSE if there are no more rows
     * This method is affected by the active fetch flag set using
     * \PhalconCassandra\Db\Result\Cassandra::setFetchMode
     * @return mixed
     */
    public function fetch()
    {
        if ($this->_fetchMode == Db::FETCH_ASSOC) {
            return $this->fetchArray();
        }

        if ($this->_numRows == 0 || $this->_numRows == $this->_seekIndex) {
            return false;
        }

        if ($this->_fetchMode == Db::FETCH_COLUMN) {
            return $this->result2column($this->_internalResult[$this->_seekIndex++]);
        }

        if ($this->_fetchMode == Db::FETCH_NUM) {
            return $this->result2num($this->_internalResult[$this->_seekIndex++]);
        }

        return $this->result2obj($this->_internalResult[$this->_seekIndex++]);
    }

    /**
     * Returns an associative array that corresponds to the fetched row,
     * or FALSE if there are no more rows.
     * @return array | null
     */
    public function fetchArray()
    {
        if ($this->_numRows == 0 || $this->_numRows == $this->_seekIndex) {
            return false;
        }
        return $this->_internalResult[$this->_seekIndex++];
    }

    /**
     * Returns an array of arrays containing all the records in the result
     * This method is affected by the active fetch flag set using
     * \PhalconCassandra\Db\Result\Cassandra::setFetchMode
     * @return array
     */
    public function fetchAll()
    {
        $final = array();
        while ($result = $this->fetch()) {
            $final[] = $result;
        }
        return $final;
    }

    /**
     * Gets number of rows returned by a resultset
     * @return int
     */
    public function numRows()
    {
        return $this->_numRows;
    }

    /**
     * Moves internal resultset cursor to another position letting us to fetch a certain row
     * <code>
     *     // Move to third row on result
     *     $result->dataSeek(2);
     * </code>
     * @param int $seekIndex - seek index starting at 0
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function dataSeek($seekIndex)
    {
        if ($seekIndex < $this->_numRows) {
            $this->_seekIndex = $seekIndex;
        } else {
            throw new CException('Data seek out of range');
        }
    }

    /**
     * Changes the fetching mode affecting PhalconCassandra\Db\Result\Cassandra::fetch()
     * <code>
     *     // Return an object
     * 	   $result->setFetchMode(\Phalcon\Db::FETCH_OBJ);
     *     // Return associative array
     * 	   $result->setFetchMode(\Phalcon\Db::FETCH_ASSOC);
     *     // Return array with integer indexes
     * 	   $result->setFetchMode(\Phalcon\Db::FETCH_NUM);
     *     // Return first column from each result row
     * 	   $result->setFetchMode(\Phalcon\Db::FETCH_COLUMN);
     * </code>
     * @param   int $mode
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function setFetchMode($mode)
    {
        if ($mode == Db::FETCH_OBJ || $mode == Db::FETCH_ASSOC || $mode == Db::FETCH_COLUMN || $mode == Db::FETCH_NUM) {
            $this->_fetchMode = $mode;
        } else {
            throw new CException('Fetch mode not supported');
        }
    }

    /**
     * Gets the internal Cassandra result object
     * @return  \Cassandra\Rows
     */
    public function getInternalResult()
    {
        return $this->_internalResult;
    }

    /**
     * Returns first column of result
     * @param   array $result
     * @return  mixed
     */
    protected function result2column(array $result)
    {
        return current($result);
    }

    /**
     * Returns indexed array result
     * @param   array $result
     * @return  array
     */
    protected function result2num(array $result)
    {
        return array_values($result);
    }

    /**
     * Returns object result
     * @param   array $result
     * @return  \stdClass
     */
    protected function result2obj(array $result)
    {
        $obj = new \stdClass();
        foreach ($result as $key => $val) {
            $obj->$key = $val;
        }
        return $obj;
    }
}
