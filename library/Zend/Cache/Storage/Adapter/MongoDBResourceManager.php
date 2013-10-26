<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace Zend\Cache\Storage\Adapter;

use MongoClient as MongoDBResource;
use Traversable;
use Zend\Cache\Exception;
use Zend\Stdlib\ArrayUtils;

/**
 * This is a resource manager for mongo
 */
class MongoDBResourceManager
{
    /**
     * Registered resources
     *
     * @var array
     */
    protected $resources = array();

    /**
     * Check if a resource exists
     *
     * @param string $id
     * @return bool
     */
    public function hasResource($id)
    {
        return isset($this->resources[$id]);
    }

    /**
     * Gets a mongo resource
     *
     * @param string $id
     * @return MongoDBResource
     * @throws Exception\RuntimeException
     */
    public function getResource($id)
    {
        if (!$this->hasResource($id)) {
            throw new Exception\RuntimeException("No resource with id '{$id}'");
        }

        $resource = $this->resources[$id];

        if ($resource instanceof MongoDBResource) {
            return $resource;
        }

        $params = $this->buildConnectionParams($resource);
        $mongoc = new MongoDBResource($params['server'], $params['options']);

        // buffer and return
        $this->resources[$id] = $mongoc;

        return $mongoc;
    }

    /**
     * Build the connection params for the MongoClient
     *
     * @param array $resource
     * @return array
     */
    protected function buildConnectionParams(array $resource)
    {
        $uri = 'mongodb://';

        foreach ($resource['servers'] as $server) {
            $uri .= $server['host'] . ':' . $server['port'];
        }

        if (isset($resource['replSetName'])) {
            $resource['options']['replicaSet'] = $resource['replSetName'];
        }

        return array('server' => $uri, 'options' => $resource['options']);
    }

    /**
     * Set servers
     *
     * $servers can be an array list or a comma separated list of servers.
     * One server in the list can be descripted as follows:
     * - Assoc: array('host' => <host>[, 'port' => <port>])
     * - List:  array(<host>[, <port>])
     *
     * @param string       $id
     * @param string|array $servers
     * @return MongoDBResourceManager
     */
    public function setServers($id, $servers)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'servers' => $servers
                ));
        }

        return $this;
    }

    /**
     * Get servers
     *
     * @param string $id
     * @throws Exception\RuntimeException
     * @return array
     */
    public function getServers($id)
    {
        if (!$this->hasResource($id)) {
            throw new Exception\RuntimeException("No resource with id '{$id}'");
        }

        $resource = & $this->resources[$id];

        if ($resource instanceof MongoDBResource) {
            /**
             * @link http://php.net/manual/mongoclient.gethosts.php
             */
            return $resource->getHosts();
        }

        return $resource['servers'];
    }

    public function setReplicaSet($replSetName)
    {

    }

    public function getReplicaSet($id)
    {

    }

    public function setDatabase($dbName)
    {

    }

    public function getDatabase($id)
    {

    }

    public function setCollection($collName)
    {

    }

    public function getCollection($id)
    {

    }

    /**
     * Set a resource
     *
     * @param string $id
     * @param array|Traversable|MongoDBResource $resource
     * @return MongoDBResourceManager Fluent interface
     * @throws Exception\InvalidArgumentException
     */
    public function setResource($id, $resource)
    {
        $id = (string) $id;

        if (!($resource instanceof MongoDBResource)) {
            if ($resource instanceof Traversable) {
                $resource = ArrayUtils::iteratorToArray($resource);
            } elseif (!is_array($resource)) {
                throw new Exception\InvalidArgumentException(
                    'Resource must be an instance of MongoClient or an array or Traversable'
                );
            }

            $resource = array_merge(
                array(
                    'servers' => array(),
                    'options' => array()
                ),
                $resource
            );

            // normalize and validate params
            $this->normalizeServers($resource['servers']);
        }

        $this->resources[$id] = $resource;

        return $this;
    }

    /**
     * Remove a resource
     *
     * @param string $id
     * @return MongoDBResourceManager Fluent interface
     */
    public function removeResource($id)
    {
        unset($this->resources[$id]);

        return $this;
    }

    /**
     * Normalize a list of servers into the following format:
     * array(array('host' => <host>, 'port' => <port>)[, ...])
     *
     * @param string|array $servers
     */
    protected function normalizeServers(& $servers)
    {
        if (!is_array($servers) && !$servers instanceof Traversable) {
            // Convert string into a list of servers
            $servers = explode(',', $servers);
        }

        $result = array();

        foreach ($servers as $server) {
            $this->normalizeServer($server);
            $result[$server['host'] . ':' . $server['port']] = $server;
        }

        $servers = array_values($result);
    }

    /**
     * Normalize one server into the following format:
     * array('host' => <host>, 'port' => <port>)
     *
     * @param string|array $server
     * @throws Exception\InvalidArgumentException
     */
    protected function normalizeServer(& $server)
    {
        $host = 'localhost';
        $port = 27017;

        // convert a single server into an array
        if ($server instanceof Traversable) {
            $server = ArrayUtils::iteratorToArray($server);
        }

        if (is_array($server)) {
            // array(<host>[, <port>])
            if (isset($server[0])) {
                $host = (string) $server[0];
                $port = isset($server[1]) ? (int) $server[1] : $port;
            }

            // array('host' => <host>[, 'port' => <port>])
            if (!isset($server[0]) && isset($server['host'])) {
                $host = (string) $server['host'];
                $port = isset($server['port']) ? (int) $server['port'] : $port;
            }
        }

        $server = array(
            'host' => $host,
            'port' => $port,
        );
    }

    /**
     * Compare 2 normalized server arrays
     * (Compares only the host and the port)
     *
     * @param array $serverA
     * @param array $serverB
     * @return int
     */
    protected function compareServers(array $serverA, array $serverB)
    {
        $keyA = $serverA['host'] . ':' . $serverA['port'];
        $keyB = $serverB['host'] . ':' . $serverB['port'];

        if ($keyA === $keyB) {
            return 0;
        }

        return $keyA > $keyB ? 1 : -1;
    }
}
