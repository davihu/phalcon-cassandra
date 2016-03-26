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
class CassandraColumn extends BaseColumn
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
     * Bind type array
     */
    const BIND_PARAM_ARRAY = 100;

}
