<?php

/*
 * Phalcon Cassandra
 * Copyright (c) 2016 David Hübner
 * This source file is subject to the New BSD License
 * Licence is bundled with this package in the file docs/LICENSE.txt
 * Author: David Hübner <david.hubner@gmail.com>
 */

namespace PhalconCassandra\Db\Dialect;

use Phalcon\Db\Dialect,
    Phalcon\Db\DialectInterface;

/**
 * Cassandra DB dialect for Phalcon
 * 
 * @author     David Hübner <david.hubner at google.com>
 * @version    Release: @package_version@
 * @since      Release 1.0
 */
class Cassandra extends Dialect implements DialectInterface
{

    public function listTables($schemaName = null)
    {
        
    }

    public function tableExists($tableName, $schemaName = null)
    {
        
    }

    public function tableOptions($table, $schema = null)
    {
        
    }

    public function createTable($tableName, $schemaName, Array $definition)
    {
        
    }

    public function dropTable($tableName, $schemaName)
    {
        
    }

    public function viewExists($viewName, $schemaName = null)
    {
        
    }

    public function createView($viewName, Array $definition, $schemaName = null)
    {
        
    }

    public function dropView($viewName, $schemaName = null, $ifExists = true)
    {
        
    }

    public function describeIndexes($table, $schema = null)
    {
        
    }

    public function addIndex($tableName, $schemaName, \Phalcon\Db\IndexInterface $index)
    {
        
    }

    public function dropIndex($tableName, $schemaName, $indexName)
    {
        
    }

    public function addPrimaryKey($tableName, $schemaName, \Phalcon\Db\IndexInterface $index)
    {
        
    }

    public function dropPrimaryKey($tableName, $schemaName)
    {
        
    }

    public function describeColumns($table, $schema = null)
    {
        
    }

    public function getColumnDefinition(\Phalcon\Db\ColumnInterface $column)
    {
        
    }

    public function addColumn($tableName, $schemaName, \Phalcon\Db\ColumnInterface $column)
    {
        
    }

    public function modifyColumn($tableName, $schemaName, \Phalcon\Db\ColumnInterface $column, \Phalcon\Db\ColumnInterface $currentColumn = null)
    {
        
    }

    public function dropColumn($tableName, $schemaName, $columnName)
    {
        
    }

    public function describeReferences($table, $schema = null)
    {
        
    }

    public function addForeignKey($tableName, $schemaName, \Phalcon\Db\ReferenceInterface $reference)
    {
        
    }

    public function dropForeignKey($tableName, $schemaName, $referenceName)
    {
        
    }

}
