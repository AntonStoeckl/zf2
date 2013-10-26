<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace Zend\Cache\Storage\Adapter;

use Zend\Cache\Exception;

/**
 * These are options specific to the MongoDB adapter
 */
class MongoDBOptions extends AdapterOptions
{
    /**
     * The mongo resource manager
     *
     * @var null|MongoDBResourceManager
     */
    protected $resourceManager;

    /**
     * The resource id of the resource manager
     *
     * @var string
     */
    protected $resourceId = 'default';

    /**
     * Set the mongo resource manager to use
     *
     * @param null|MongoDBResourceManager $resourceManager
     * @return MongoDBOptions
     */
    public function setResourceManager(MongoDBResourceManager $resourceManager = null)
    {
        if ($this->resourceManager !== $resourceManager) {
            $this->triggerOptionEvent('resource_manager', $resourceManager);
            $this->resourceManager = $resourceManager;
        }

        return $this;
    }

    /**
     * Get the memcached resource manager
     *
     * @return MongoDBResourceManager
     */
    public function getResourceManager()
    {
        if (!$this->resourceManager) {
            $this->resourceManager = new MongoDBResourceManager();
        }

        return $this->resourceManager;
    }

    /**
     * Get the memcached resource id
     *
     * @return string
     */
    public function getResourceId()
    {
        return $this->resourceId;
    }

    /**
     * Set the memcached resource id
     *
     * @param string $resourceId
     * @return MongoDBOptions
     */
    public function setResourceId($resourceId)
    {
        $resourceId = (string) $resourceId;

        if ($this->resourceId !== $resourceId) {
            $this->triggerOptionEvent('resource_id', $resourceId);
            $this->resourceId = $resourceId;
        }

        return $this;
    }

    /**
    * Set a list of mongo servers to add on initialize
    *
    * @param string|array $servers list of servers
    * @return MongoDBOptions
    */
    public function setServers($servers)
    {
        $this->getResourceManager()->setServers($this->getResourceId(), $servers);

        return $this;
    }

    /**
     * Get Servers
     *
     * @return array
     */
    public function getServers()
    {
        return $this->getResourceManager()->getServers($this->getResourceId());
    }

    public function setReplicaSet($replSetName)
    {
        $this->getResourceManager()->setServers($this->getResourceId(), $replSetName);
    }

    public function getReplicaSet()
    {
        return $this->getResourceManager()->getReplicaSet($this->getResourceId());
    }

    public function setDatabase($dbName)
    {
        $this->getResourceManager()->setServers($this->getResourceId(), $dbName);
    }

    public function getDatabase()
    {
        return $this->getResourceManager()->getDatabase($this->getResourceId());
    }

    public function setCollection($collName)
    {
        $this->getResourceManager()->setServers($this->getResourceId(), $collName);
    }

    public function getCollection()
    {
        return $this->getResourceManager()->getCollection($this->getResourceId());
    }
}
