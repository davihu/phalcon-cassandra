<?php

/*
 * Phalcon Cassandra
 * Copyright (c) 2016 David Hübner
 * This source file is subject to the New BSD License
 * Licence is bundled with this package in the file docs/LICENSE.txt
 * Author: David Hübner <david.hubner@gmail.com>
 */

namespace PhalconCassandra\Db\Dialect;

use PhalconCassandra\Db\Exception\Cassandra as CException,
    Phalcon\Db\Dialect,
    Phalcon\Db\DialectInterface,
    Phalcon\Db\IndexInterface,
    Phalcon\Db\ColumnInterface;

/**
 * Cassandra DB dialect for Phalcon
 * 
 * @author     David Hübner <david.hubner at google.com>
 * @version    Release: @package_version@
 * @since      Release 1.0
 */
class Cassandra extends Dialect implements DialectInterface
{

    public function limit($sqlQuery, $number)
    {
        return $sqlQuery;
    }

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

    /**
     * Views not supported by this adapter version  
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function listViews($schemaName = null)
    {
        throw new CException('Not supported - list views');
    }

    /**
     * Views not supported by this adapter version  
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function viewExists($viewName, $schemaName = null)
    {
        throw new CException('Not supported - view exists');
    }

    /**
     * Views not supported by this adapter version
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function createView($viewName, Array $definition, $schemaName = null)
    {
        throw new CException('Not nupported - drop view');
    }

    /**
     * Views not supported by this adapter version
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function dropView($viewName, $schemaName = null, $ifExists = true)
    {
        throw new CException('Not nupported - drop view');
    }

    /**
     * Indexes not supported by this adapter version
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function describeIndexes($table, $schema = null)
    {
        throw new CException('Not nupported - describe indexes');
    }

    /**
     * Indexes not supported by this adapter version
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function addIndex($tableName, $schemaName, IndexInterface $index)
    {
        throw new CException('Not nupported - add index');
    }

    /**
     * Indexes not supported by this adapter version
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function dropIndex($tableName, $schemaName, $indexName)
    {
        throw new CException('Not nupported - drop index');
    }

    /**
     * Primary key modification not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function addPrimaryKey($tableName, $schemaName, IndexInterface $index)
    {
        throw new CException('Not nupported - add primary key');
    }

    /**
     * Primary key modification not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function dropPrimaryKey($tableName, $schemaName)
    {
        throw new CException('Not nupported - drop primary key');
    }

    /**
     * No CQL syntax for describing a table
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function describeColumns($table, $schema = null)
    {
        throw new CException('Not nupported - describe columns');
    }

    public function getColumnDefinition(ColumnInterface $column)
    {
        
    }

    public function addColumn($tableName, $schemaName, ColumnInterface $column)
    {
        
    }

    public function modifyColumn($tableName, $schemaName, ColumnInterface $column, ColumnInterface $currentColumn = null)
    {
        
    }

    public function dropColumn($tableName, $schemaName, $columnName)
    {
        
    }

    /**
     * Savepoints not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function supportsSavepoints()
    {
        return false;
    }

    /**
     * Savepoints not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @return  bool
     */
    public function supportsReleaseSavepoints()
    {
        return false;
    }

    /**
     * Savepoints not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function createSavepoint($name)
    {
        throw new CException('Not nupported - create savepoint');
    }

    /**
     * Savepoints not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function releaseSavepoint($name)
    {
        throw new CException('Not nupported - release savepoint');
    }

    /**
     * Savepoints not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function rollbackSavepoint($name)
    {
        throw new CException('Not nupported - rollback savepoint');
    }

    /**
     * Foreign keys not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function describeReferences($table, $schema = null)
    {
        throw new CException('Not nupported - describe references');
    }

    /**
     * Foreign keys not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function addForeignKey($tableName, $schemaName, \Phalcon\Db\ReferenceInterface $reference)
    {
        throw new CException('Not nupported - add foreign key');
    }

    /**
     * Foreign keys not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function dropForeignKey($tableName, $schemaName, $referenceName)
    {
        throw new CException('Not nupported - drop foreign key');
    }

    /**
     * For update not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function forUpdate($sqlQuery)
    {
        throw new CException('Not nupported - for update');
    }

    /**
     * Shared lock not supported by Cassandra
     *
     * @author  David Hübner <david.hubner at google.com>
     * @throws  \PhalconCassandra\Db\Exception\Cassandra
     */
    public function sharedLock($sqlQuery)
    {
        throw new CException('Not nupported - shared lock');
    }

}
