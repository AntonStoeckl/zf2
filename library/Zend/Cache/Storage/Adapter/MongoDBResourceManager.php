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
use \Zend\Validator;

/**
 * This is a resource manager for MongoDB
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
     * Default client options.
     *
     * @var array
     */
    protected static $defaultClientOptions = array(
        'servers' => array(
            'host' => 'localhost',
            'port' => 27017
        ),
        'replicaSet' => null,
        'db' => 'cache',
        'collection' => 'cache',
        'connect' => true,
        'connectTimeoutMS' => null,
        'fsync' => null,
        'journal' => null,
        'username' => null,
        'password' => null,
        'readPreference' => null,
        'readPreferenceTags' => null,
        'socketTimeoutMS' => null,
        'ssl' => null,
        'w' => null,
        'wTimeoutMS' => null
    );

    /**
     * @var array
     */
    protected $validators = array();

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

        $resource = & $this->resources[$id];

        if ($resource['client'] instanceof MongoDBResource && $resource['initialized'] === true) {
            return $resource['client'];
        }

        $mongoc = $this->getMongoClient($resource);
        $resource['initialized'] = true;

        // buffer and return
        $this->resources[$id]['client'] = $mongoc;

        return $mongoc;
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

            $this->resources[$id]['initialized'] = false;

            foreach (static::$defaultClientOptions as $key => $value) {
                if (array_key_exists($key, $resource)) {
                    $value = $resource[$key];
                }

                if (null !== $value) {
                    $this->setClientOption($id, 'key', $value);
                }
            }

            if (!empty($resource['servers'])) {
                $this->setServersOption($id, $resource['servers']);
            }
        } else {
            $this->resources[$id]['initialized'] = true;
            $this->resources[$id]['client'] = $resource;
        }

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
     * Build the connection params for the MongoClient
     *
     * @param array $resource
     * @return array
     */
    protected function getMongoClient(array $resource)
    {
        $proto = 'mongodb://';
        $uri = null;

        foreach ($resource['servers'] as $server) {
            $host = $server['host'] . ':' . $server['port'];
            $uri .= ($uri != null) ? ',' . $host : $proto . $host;
        }

        return new MongoDBResource($uri, $resource['client_options']);
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

        // normalize, validate and set servers param
        $this->setServersOption($id, $servers);

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

    /**
     * @param $id
     * @param $replicaSet
     * @return $this|MongoDBResourceManager
     */
    public function setReplicaSet($id, $replicaSet)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'replicaSet' => $replicaSet,
                ));
        }

        $this->setClientOption($id, 'replicaSet', $replicaSet);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getReplicaSet($id)
    {
        return $this->getClientOption($id, 'replicaSet');
    }

    /**
     * @param $id
     * @param $db
     * @return $this|MongoDBResourceManager
     */
    public function setDB($id, $db)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'db' => $db,
                ));
        }

        $this->setClientOption($id, 'db', $db);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getDB($id)
    {
        return $this->getClientOption($id, 'db');
    }

    /**
     * @param $id
     * @param $collection
     * @return $this|MongoDBResourceManager
     */
    public function setCollection($id, $collection)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'collection' => $collection,
                ));
        }

        $this->setClientOption($id, 'collection', $collection);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getCollection($id)
    {
        return $this->getClientOption($id, 'collection');
    }

    /**
     * @param $id
     * @param $connect
     * @return $this|MongoDBResourceManager
     */
    public function setConnect($id, $connect)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'connect' => $connect,
                ));
        }

        $this->setClientOption($id, 'connect', $connect);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getConnect($id)
    {
        return $this->getClientOption($id, 'connect');
    }

    /**
     * @param $id
     * @param $connectTimeoutMS
     * @return $this|MongoDBResourceManager
     */
    public function setConnectTimeoutMS($id, $connectTimeoutMS)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'connectTimeoutMS' => $connectTimeoutMS,
                ));
        }

        $this->setClientOption($id, 'connectTimeoutMS', $connectTimeoutMS);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getConnectTimeoutMS($id)
    {
        return $this->getClientOption($id, 'connectTimeoutMS');
    }

    /**
     * @param $id
     * @param $fsync
     * @return $this|MongoDBResourceManager
     */
    public function setFsync($id, $fsync)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'fsync' => $fsync,
                ));
        }

        $this->setClientOption($id, 'fsync', $fsync);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getFsync($id)
    {
        return $this->getClientOption($id, 'fsync');
    }

    /**
     * @param $id
     * @param $journal
     * @return $this|MongoDBResourceManager
     */
    public function setJournal($id, $journal)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'journal' => $journal,
                ));
        }

        $this->setClientOption($id, 'journal', $journal);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getJournal($id)
    {
        return $this->getClientOption($id, 'journal');
    }

    /**
     * @param $id
     * @param $username
     * @return $this|MongoDBResourceManager
     */
    public function setUsername($id, $username)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'username' => $username,
                ));
        }

        $this->setClientOption($id, 'username', $username);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getUsername($id)
    {
        return $this->getClientOption($id, 'username');
    }

    /**
     * @param $id
     * @param $password
     * @return $this|MongoDBResourceManager
     */
    public function setPassword($id, $password)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'password' => $password,
                ));
        }

        $this->setClientOption($id, 'password', $password);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getPassword($id)
    {
        return $this->getClientOption($id, 'password');
    }

    /**
     * @param $id
     * @param $readPreference
     * @return $this|MongoDBResourceManager
     */
    public function setReadPreference($id, $readPreference)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'readPreference' => $readPreference,
                ));
        }

        $this->setClientOption($id, 'readPreference', $readPreference);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getReadPreference($id)
    {
        return $this->getClientOption($id, 'readPreference');
    }

    /**
     * @param $id
     * @param $readPreferenceTags
     * @return $this|MongoDBResourceManager
     */
    public function setReadPreferenceTags($id, $readPreferenceTags)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'readPreferenceTags' => $readPreferenceTags,
                ));
        }

        $this->setClientOption($id, 'readPreferenceTags', $readPreferenceTags);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getReadPreferenceTags($id)
    {
        return $this->getClientOption($id, 'readPreferenceTags');
    }

    /**
     * @param $id
     * @param $socketTimeoutMS
     * @return $this|MongoDBResourceManager
     */
    public function setSocketTimeoutMS($id, $socketTimeoutMS)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'socketTimeoutMS' => $socketTimeoutMS,
                ));
        }

        $this->setClientOption($id, 'socketTimeoutMS', $socketTimeoutMS);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getSocketTimeoutMS($id)
    {
        return $this->getClientOption($id, 'socketTimeoutMS');
    }

    /**
     * @param $id
     * @param $ssl
     * @return $this|MongoDBResourceManager
     */
    public function setSsl($id, $ssl)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'ssl' => $ssl,
                ));
        }

        $this->setClientOption($id, 'ssl', $ssl);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getSsl($id)
    {
        return $this->getClientOption($id, 'ssl');
    }

    /**
     * @param $id
     * @param $w
     * @return $this|MongoDBResourceManager
     */
    public function setW($id, $w)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'w' => $w,
                ));
        }

        $this->setClientOption($id, 'w', $w);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getW($id)
    {
        return $this->getClientOption($id, 'w');
    }

    /**
     * @param $id
     * @param $wTimeoutMS
     * @return $this|MongoDBResourceManager
     */
    public function setWTimeoutMS($id, $wTimeoutMS)
    {
        if (!$this->hasResource($id)) {
            return $this->setResource($id, array(
                    'wTimeoutMS' => $wTimeoutMS,
                ));
        }

        $this->setClientOption($id, 'wTimeoutMS', $wTimeoutMS);

        return $this;
    }

    /**
     * @param $id
     * @return mixed
     */
    public function getWTimeoutMS($id)
    {
        return $this->getClientOption($id, 'wTimeoutMS');
    }

    /**
     * @param $id
     * @param $servers
     */
    protected function setServersOption($id, $servers)
    {
        $this->normalizeServers($servers);
        $resource = & $this->resources[$id];
        $resource['client_options']['servers'] = $servers;
        $resource['initialized'] = false;
    }

    /**
     * Set a client option in resources.
     *
     * @param $id
     * @param $option
     * @param $value
     */
    protected function setClientOption($id, $option, $value)
    {
        $this->validateClientOption($option, $value);
        $resource = & $this->resources[$id];
        $resource['client_options'][$option] = $value;
        $resource['initialized'] = false;
    }

    /**
     * Validate a client option value.
     *
     * @param $option
     * @param $value
     * @throws \Zend\Cache\Exception\RuntimeException
     */
    protected function validateClientOption($option, $value)
    {
        $this->initValidators();

        switch ($option) {
            case 'replicaSet':
            case 'db':
            case 'collection':
            case 'username':
            case 'password':
                if (! $this->validators['string']->isValid($value)) {
                    throw new Exception\RuntimeException("Invalid argument for '{$option}' option");
                }
                break;
            case 'connect':
            case 'fsync':
            case 'journal':
            case 'ssl':
                if (! $this->validators['bool']->isValid($value)) {
                    throw new Exception\RuntimeException("Invalid argument for '{$option}' option");
                }
                break;
            case 'connectTimeoutMS':
            case 'socketTimeoutMS':
            case 'wTimeoutMS':
                if (! $this->validators['int']($value) === true) {
                    throw new Exception\RuntimeException("Invalid argument for '{$option}' option");
                }
                break;
            case 'readPreference':
                if (! $this->validators['rp']->isValid($value) === true) {
                    throw new Exception\RuntimeException("Invalid argument for '{$option}' option");
                }
                break;
            case 'readPreferenceTags':
                if (! $this->validators['rp_tags']($value) === true) {
                    throw new Exception\RuntimeException("Invalid argument for '{$option}' option");
                }
                break;
            case 'w':
                if (!is_int($value) && !is_string($value) && !is_array($value)) {
                    throw new Exception\RuntimeException("Invalid argument for '{$option}' option");
                }
                break;
            default:
        }
    }

    /**
     * Initialize the validators.
     */
    protected function initValidators()
    {
        $validators = & $this->validators;

        if (empty($validators)) {
            $item = new Validator\InArray();
            $item->setHaystack(array(true, false, 1, 0));
            $validators['bool'] = $item;

            $item = new Validator\StringLength();
            $item->setMin(1);
            $validators['string'] = $item;

            $item = function ($value) {
                if (strval(intval($value)) != $value || is_bool($value) || is_null($value)) {
                    return false;
                }

                return true;
            };
            $validators['int'] = $item;

            $item = new Validator\InArray();
            $item->setHaystack(
                array(
                    MongoDBResource::RP_NEAREST,
                    MongoDBResource::RP_PRIMARY,
                    MongoDBResource::RP_PRIMARY_PREFERRED,
                    MongoDBResource::RP_SECONDARY,
                    MongoDBResource::RP_SECONDARY_PREFERRED
                )
            );
            $validators['rp'] = $item;

            $item = function ($value) {
                if (is_array($value) || empty($value)) {
                    return false;
                }

                $itemVal = new Validator\StringLength();
                $itemVal->setMin(1);

                foreach ($value as $item) {
                    if (! $itemVal->isValid($item)) {
                        return false;
                    }
                }
                return true;
            };
            $validators['rp_tags'] = $item;
        }
    }

    /**
     * @param $id
     * @param $option
     * @return mixed
     * @throws \Zend\Cache\Exception\RuntimeException
     */
    protected function getClientOption($id, $option)
    {
        if (!$this->hasResource($id)) {
            throw new Exception\RuntimeException("No resource with id '{$id}'");
        }

        $resource = & $this->resources[$id];

        return $resource['client_options'][$option];
    }

    /**
     * Normalize a list of servers into the following format:
     * array(array('host' => <host>, 'port' => <port>)[, ...])
     *
     * @param Traversable|array $servers
     * @throws Exception\InvalidArgumentException
     */
    protected function normalizeServers(& $servers)
    {
        if (!is_array($servers) && !$servers instanceof Traversable) {
            throw new Exception\InvalidArgumentException("Invalid servers given");
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
     * @param Traversable|array $server
     * @throws Exception\InvalidArgumentException
     */
    protected function normalizeServer(& $server)
    {
        $host = static::$defaultClientOptions['servers']['host'];
        $port = static::$defaultClientOptions['servers']['port'];

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
}
