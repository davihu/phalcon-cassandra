<?php

/*
 * Phalcon Cassandra
 * Copyright (c) 2016 David Hübner
 * This source file is subject to the New BSD License
 * Licence is bundled with this package in the file docs/LICENSE.txt
 * Author: David Hübner <david.hubner@gmail.com>
 */

namespace PhalconCassandra\Db\Column;

use Phalcon\Db\Column as BaseColumn;

/**
 * Extended Phalcon DB column with added support for Cassandra data types
 * 
 * @author     David Hübner <david.hubner at google.com>
 * @version    Release: @package_version@
 * @since      Release 1.0
 */
class Cassandra extends BaseColumn
{

    /**
     * UUID data type 
     */
    const TYPE_UUID = 100;

    /**
     * TimeUUID data type 
     */
    const TYPE_TIMEUUID = 101;

    /**
     * Counter data type
     */
    const TYPE_COUNTER = 102;

    /**
     * Ascii string data type
     */
    const TYPE_ASCII = 103;

    /**
     * IP address data type
     */
    const TYPE_INET = 104;

    /**
     * Set data type
     */
    const TYPE_SET = 105;

    /**
     * List data type
     */
    const TYPE_LIST = 106;

    /**
     * Map data type
     */
    const TYPE_MAP = 107;

    /**
     * Varint data type
     */
    const TYPE_VARINT = 108;

    /**
     * Bind type UUID
     */
    const BIND_PARAM_UUID = 100;

    /**
     * Bind type array
     */
    const BIND_PARAM_ARRAY = 101;

    /**
     * @var boolean $_partitionKey - whether the column is part of partition key
     */
    protected $_partitionKey = false;

    /**
     * @var boolean $_clusteringKey - whether the column is clustering key
     */
    protected $_clusteringKey = false;

    /**
     * @var boolean $_reversed - whether the column is in descending or ascending order
     */
    protected $_reversed = false;

    /**
     * @var boolean $_static - whether the column is static
     */
    protected $_static = false;

    /**
     * @var boolean $_frozen - whether the column is frozen
     */
    protected $_frozen = false;

    /**
     * Creates new cassandra column
     * @param   string $name
     * @param   array $definition
     */
    public function __construct($name, $definition)
    {
        parent::__construct($name, $definition);
        if (!empty($definition['autoIncrement'])) {
            throw new CException('Auto increment columns not supported');
        }
        if (!empty($definition['partitionKey'])) {
            $this->_partitionKey = true;
        }
        if (!empty($definition['clusteringKey'])) {
            $this->_clusteringKey = true;
        }
        if (!empty($definition['reversed'])) {
            $this->_reversed = true;
        }
        if (!empty($definition['static'])) {
            $this->_static = true;
        }
        if (!empty($definition['frozen'])) {
            $this->_frozen = true;
        }
    }

    /**
     * Returns whether the column is part of partition key
     * @return  bool
     */
    public function isPartitionKey()
    {
        return $this->_partitionKey;
    }

    /**
     * Returns whether the column is clustering key
     * @return  bool
     */
    public function isClusteringKey()
    {
        return $this->_clusteringKey;
    }

    /**
     * Returns whether the column is in descending or ascending order
     * @return  bool
     */
    public function isReversed()
    {
        return $this->_reversed;
    }

    /**
     * Returns whether the column is static
     * @return  bool
     */
    public function isStatic()
    {
        return $this->_static;
    }

    /**
     * Returns whether the column is frozen
     * @return  bool
     */
    public function isFrozen()
    {
        return $this->_frozen;
    }
}
