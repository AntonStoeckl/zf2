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

    public function setReplicaSet($replicaSet)
    {
        $this->getResourceManager()->setReplicaSet($this->getResourceId(), $replicaSet);

        return $this;
    }

    public function getReplicaSet()
    {
        return $this->getResourceManager()->getReplicaSet($this->getResourceId());
    }

    public function setDB($db)
    {
        $this->getResourceManager()->setDB($this->getResourceId(), $db);

        return $this;
    }

    public function getDB()
    {
        return $this->getResourceManager()->getDB($this->getResourceId());
    }

    public function setCollection($collection)
    {
        $this->getResourceManager()->setCollection($this->getResourceId(), $collection);

        return $this;
    }

    public function getCollection()
    {
        return $this->getResourceManager()->getCollection($this->getResourceId());
    }

    public function setConnectTimeoutMS($connectTimeoutMS)
    {
        $this->getResourceManager()->setConnectTimeoutMS($this->getResourceId(), $connectTimeoutMS);

        return $this;
    }

    public function getConnectTimeoutMS()
    {
        return $this->getResourceManager()->getConnectTimeoutMS($this->getResourceId());
    }

    public function setFsync($fsync)
    {
        $this->getResourceManager()->setFsync($this->getResourceId(), $fsync);

        return $this;
    }

    public function getFsync()
    {
        return $this->getResourceManager()->getFsync($this->getResourceId());
    }

    public function setJournal($journal)
    {
        $this->getResourceManager()->setJournal($this->getResourceId(), $journal);

        return $this;
    }

    public function getJournal()
    {
        return $this->getResourceManager()->getJournal($this->getResourceId());
    }

    public function setUsername($username)
    {
        $this->getResourceManager()->setUsername($this->getResourceId(), $username);

        return $this;
    }

    public function getUsername()
    {
        return $this->getResourceManager()->getUsername($this->getResourceId());
    }

    public function setPassword($password)
    {
        $this->getResourceManager()->setPassword($this->getResourceId(), $password);

        return $this;
    }

    public function getPassword()
    {
        return $this->getResourceManager()->getPassword($this->getResourceId());
    }

    public function setReadPreference($readPreference)
    {
        $this->getResourceManager()->setReadPreference($this->getResourceId(), $readPreference);

        return $this;
    }

    public function getReadPreference()
    {
        return $this->getResourceManager()->getReadPreference($this->getResourceId());
    }

    public function setReadPreferenceTags($readPreferenceTags)
    {
        $this->getResourceManager()->setReadPreferenceTags($this->getResourceId(), $readPreferenceTags);

        return $this;
    }

    public function getReadPreferenceTags()
    {
        return $this->getResourceManager()->getReadPreferenceTags($this->getResourceId());
    }

    public function setSocketTimeoutMS($socketTimeoutMS)
    {
        $this->getResourceManager()->setSocketTimeoutMS($this->getResourceId(), $socketTimeoutMS);

        return $this;
    }

    public function getSocketTimeoutMS()
    {
        return $this->getResourceManager()->getSocketTimeoutMS($this->getResourceId());
    }

    public function setSsl($ssl)
    {
        $this->getResourceManager()->setSsl($this->getResourceId(), $ssl);

        return $this;
    }

    public function getSsl()
    {
        return $this->getResourceManager()->getSsl($this->getResourceId());
    }

    public function setW($w)
    {
        $this->getResourceManager()->setW($this->getResourceId(), $w);

        return $this;
    }

    public function getW()
    {
        return $this->getResourceManager()->getW($this->getResourceId());
    }

    public function setWTimeoutMS($wTimeoutMS)
    {
        $this->getResourceManager()->setWTimeoutMS($this->getResourceId(), $wTimeoutMS);

        return $this;
    }

    public function getWTimeoutMS()
    {
        return $this->getResourceManager()->getWTimeoutMS($this->getResourceId());
    }
}
