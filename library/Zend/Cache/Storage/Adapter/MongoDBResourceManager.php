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
     * @var array
     */
    protected static $defaultServer = array(
        'host' => '127.0.0.1',
        'port' => 27017
    );

    /**
     * @var string
     */
    protected static $defaultCollection = 'zfcache';

    /**
     * Default client options.
     *
     * @var array
     */
    protected static $defaultClientOptions = array(
        'replicaSet'         => null,
        'db'                 => 'zfcache',
        'connect'            => true,
        'connectTimeoutMS'   => null,
        'fsync'              => null,
        'journal'            => null,
        'username'           => null,
        'password'           => null,
        'readPreference'     => null,
        'readPreferenceTags' => null,
        'socketTimeoutMS'    => null,
        'ssl'                => null,
        'w'                  => 1,
        'wTimeoutMS'         => null
    );

    /**
     * @var array
     */
    protected $validators = array();

    /**
     * Check if a resource exists
     *
     * @param string $resourceId
     * @return bool
     */
    public function hasResource($resourceId)
    {
        return isset($this->resources[$resourceId]);
    }

    /**
     * Gets a mongo resource
     *
     * @param string $resourceId
     * @return MongoDBResource
     * @throws Exception\RuntimeException
     */
    public function getResource($resourceId)
    {
        if (! $this->hasResource($resourceId)) {
            throw new Exception\RuntimeException("No resource with id '{$resourceId}'");
        }

        $resource = & $this->resources[$resourceId];

        if (array_key_exists('client', $resource)) {
            if ($resource['client'] instanceof MongoDBResource && $resource['initialized'] === true) {
                return $resource['client'];
            }
        }

        $mongoc = $this->getMongoClient($resource);
        $resource['initialized'] = true;

        // buffer and return
        $this->resources[$resourceId]['client'] = $mongoc;

        return $mongoc;
    }

    /**
     * Set a resource
     *
     * @param string $resourceId
     * @param array|Traversable|MongoDBResource $resource
     * @return MongoDBResourceManager Fluent interface
     * @throws Exception\InvalidArgumentException
     */
    public function setResource($resourceId, $resource)
    {
        $resourceId = (string) $resourceId;

        if (! $resource instanceof MongoDBResource) {
            if ($resource instanceof Traversable) {
                $resource = ArrayUtils::iteratorToArray($resource);
            } elseif (! is_array($resource)) {
                throw new Exception\InvalidArgumentException(
                    'Resource must be an instance of MongoClient or an array or Traversable'
                );
            }

            $this->resources[$resourceId]['initialized'] = false;

            foreach (static::$defaultClientOptions as $key => $value) {
                if (array_key_exists($key, $resource)) {
                    $value = $resource[$key];
                }

                if (null !== $value) {
                    $this->doSetClientOption($resourceId, $key, $value);
                }
            }

            if (! empty($resource['servers'])) {
                $this->setServersOption($resourceId, $resource['servers']);
            } else {
                $this->setServersOption(
                    $resourceId,
                    array(
                        array(
                            'host' => self::$defaultServer['host'],
                            'port' => self::$defaultServer['port']
                        )
                    )
                );
            }

            if (! empty($resource['collection'])) {
                $this->setCollection($resourceId, $resource['collection']);
            } else {
                $this->setCollection($resourceId, self::$defaultCollection);
            }
        } else {
            $this->resources[$resourceId]['initialized'] = true;
            $this->resources[$resourceId]['client'] = $resource;
        }

        return $this;
    }

    /**
     * Remove a resource
     *
     * @param string $resourceId
     * @return MongoDBResourceManager Fluent interface
     */
    public function removeResource($resourceId)
    {
        unset($this->resources[$resourceId]);

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
     * @param string       $resourceId
     * @param string|array $servers
     * @return MongoDBResourceManager
     */
    public function setServers($resourceId, $servers)
    {
        if (! $this->hasResource($resourceId)) {
            return $this->setResource($resourceId, array(
                    'servers' => $servers
                ));
        }

        // normalize, validate and set servers param
        $this->setServersOption($resourceId, $servers);

        return $this;
    }

    /**
     * Setter for field 'servers' in the resources array.
     *
     * @param string $resourceId
     * @param Traversable|array $servers
     */
    protected function setServersOption($resourceId, $servers)
    {
        $this->normalizeServers($servers);
        $resource = & $this->resources[$resourceId];
        $resource['servers'] = $servers;
        $resource['initialized'] = false;
    }

    /**
     * Get servers
     *
     * @param string $resourceId
     * @throws Exception\RuntimeException
     * @return array
     */
    public function getServers($resourceId)
    {
        if (! $this->hasResource($resourceId)) {
            throw new Exception\RuntimeException("No resource with id '{$resourceId}'");
        }

        $resource = & $this->resources[$resourceId];

        if ($resource['client'] instanceof MongoDBResource) {
            /**
             * @link http://php.net/manual/mongoclient.gethosts.php
             */
            return $resource['client']->getHosts();
        }

        return $resource['servers'];
    }

    /**
     * Setter for field 'collection' in the resources array.
     *
     * @param string $resourceId
     * @param string $collection
     * @return $this|MongoDBResourceManager
     */
    public function setCollection($resourceId, $collection)
    {
        if (! $this->hasResource($resourceId)) {
            return $this->setResource($resourceId, array(
                    'collection' => $collection,
                ));
        }

        if (($result = $this->validateClientOption('collection', $collection) !== true)) {
            $collection = $result;
        }

        $resource = & $this->resources[$resourceId];
        $resource['collection'] = $collection;
        $resource['initialized'] = false;

        return $this;
    }

    /**
     * Getter for field 'collection' in the resources array.
     *
     * @param string $resourceId
     * @return string
     * @throws Exception\RuntimeException
     */
    public function getCollection($resourceId)
    {
        if (! $this->hasResource($resourceId)) {
            throw new Exception\RuntimeException("No resource with id '{$resourceId}'");
        }

        $resource = & $this->resources[$resourceId];

        return $resource['collection'];
    }

    /**
     * Magic method, so we don't have to implement getters and setters for each possible
     * option for @see \MongoClient.
     * This only accepts methods that start with get or set, followed by a capital letter and calls
     * @see MongoDBResourceManager::setClientOption()
     * or
     * @see MongoDBResourceManager::getClientOption()
     * with the extracted option name and the supplied args array.
     *
     * @param string $methodName
     * @param array $args
     * @return $this|mixed
     * @throws Exception\InvalidArgumentException
     */
    public function __call($methodName, array $args)
    {
        if (preg_match('/^set([A-Z][a-zA-Z]*)$/', $methodName, $matches)) {
            $optionName = lcfirst($matches[1]);
            $result = $this->setClientOption($optionName, $args);
        } elseif (preg_match('/^get([A-Z][a-zA-Z]*)$/', $methodName, $matches)) {
            $optionName = lcfirst($matches[1]);
            $result = $this->getClientOption($optionName, $args);
        } else {
            throw new Exception\InvalidArgumentException('Invalid method name called');
        }

        return $result;
    }

    /**
     * Proxy setter for client options, this is called from
     * @see MongoDBResourceManager::__call()
     * It evaluates the number of args and calls
     * @see MongoDBResourceManager::doSetClientOption()
     *
     * @param string $optionName
     * @param array $args
     * @return $this
     * @throws Exception\InvalidArgumentException
     */
    protected function setClientOption($optionName, array $args)
    {
        if (count($args) !== 2) {
            throw new Exception\InvalidArgumentException(
                'Invalid argument count, "resourceId" and "optionValue" required'
            );
        }

        $resourceId = array_shift($args);
        $optionValue = array_shift($args);

        if (! $this->hasResource($resourceId)) {
            return $this->setResource($resourceId, array(
                    $optionName => $optionValue,
                ));
        }

        $this->doSetClientOption($resourceId, $optionName, $optionValue);

        return $this;
    }

    /**
     * Proxy getter for client options, this is called from
     * @see MongoDBResourceManager::__call()
     * It evaluates the number of args and calls
     * @see MongoDBResourceManager::doGetClientOption()
     *
     * @param string $optionName
     * @param array $args
     * @return mixed
     * @throws Exception\InvalidArgumentException
     */
    protected function getClientOption($optionName, array $args)
    {
        if (count($args) !== 1) {
            throw new Exception\InvalidArgumentException(
                'Invalid argument count, "resourceId" required'
            );
        }

        $resourceId = array_shift($args);

        return $this->doGetClientOption($resourceId, $optionName);
    }

    /**
     * Real getter for client options.
     * Checks first if a resource with this id exists.
     *
     * @param string $resourceId
     * @param string $option
     * @return mixed
     * @throws Exception\RuntimeException
     */
    protected function doGetClientOption($resourceId, $option)
    {
        if (! $this->hasResource($resourceId)) {
            throw new Exception\RuntimeException("No resource with id '{$resourceId}'");
        }

        $resource = & $this->resources[$resourceId];

        return $resource['client_options'][$option];
    }

    /**
     * Real setter for client options in resources array.
     *
     * @param string $resourceId
     * @param string $option
     * @param mixed $value
     */
    protected function doSetClientOption($resourceId, $option, $value)
    {
        $this->validateClientOption($option, $value);
        $resource = & $this->resources[$resourceId];
        $resource['client_options'][$option] = $value;
        $resource['initialized'] = false;
    }

    /**
     * Validate a client option value.
     * Return bool if value is valid.
     * Some options return a sane default if the value is invalid.
     *
     * @param string $option
     * @param mixed $value
     * @return bool|string
     * @throws Exception\InvalidArgumentException
     */
    protected function validateClientOption($option, $value)
    {
        $this->initValidators();

        switch ($option) {
            case 'replicaSet':
            case 'db':
            case 'username':
            case 'password':
                if (! $this->validators['string']->isValid($value)) {
                    throw new Exception\InvalidArgumentException("Invalid argument for '{$option}' option");
                }
                break;
            case 'collection':
                if (! $this->validators['string']->isValid($value)) {
                    return self::$defaultCollection;
                }
                break;
            case 'connect':
            case 'fsync':
            case 'journal':
            case 'ssl':
                if (! $this->validators['bool']->isValid($value)) {
                    throw new Exception\InvalidArgumentException("Invalid argument for '{$option}' option");
                }
                break;
            case 'connectTimeoutMS':
            case 'socketTimeoutMS':
            case 'wTimeoutMS':
                if (! $this->validators['int']($value) === true) {
                    throw new Exception\InvalidArgumentException("Invalid argument for '{$option}' option");
                }
                break;
            case 'readPreference':
                if (! $this->validators['rp']->isValid($value) === true) {
                    throw new Exception\InvalidArgumentException("Invalid argument for '{$option}' option");
                }
                break;
            case 'readPreferenceTags':
                if (! $this->validators['rp_tags']($value) === true) {
                    throw new Exception\InvalidArgumentException("Invalid argument for '{$option}' option");
                }
                break;
            case 'w':
                if (! $this->validators['w']($value) === true) {
                    throw new Exception\InvalidArgumentException("Invalid argument for '{$option}' option");
                }
                break;
            default:
                throw new Exception\InvalidArgumentException("Unknown client option: '{$option}'");
        }

        return true;
    }

    /**
     * Initialize the validators.
     * Some are Zend\Validator(s), some are anonymous functions.
     */
    protected function initValidators()
    {
        $validators = & $this->validators;

        if (empty($validators)) {
            /**
             * Boolean validator, also accepts 1 and 0
             */
            $item = new Validator\InArray();
            $item->setHaystack(array(true, false, 1, 0));
            $validators['bool'] = $item;

            /**
             * String validator.
             */
            $item = new Validator\StringLength();
            $item->setMin(1);
            $validators['string'] = $item;

            /**
             * Integer validator, also accepting integerish strings
             */
            $item = function ($value) {
                if (strval(intval($value)) != $value || is_bool($value) || is_null($value)) {
                    return false;
                }

                return true;
            };
            $validators['int'] = $item;

            /**
             * Validates MongoDB "Read Preferences"
             */
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

            /**
             * Validates MongoDB "Read Preference Tags"
             */
            $item = function ($value) {
                if (! is_array($value) || empty($value)) {
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

            /**
             * Validates MongoDB "Write Concerns"
             */
            $item = function ($value) use (& $validators) {
                if ($validators['int']($value) === true) {
                    if (1 <= $value) {
                        return true;
                    }
                } elseif (is_array($value)) {
                    foreach ($value as $vItem) {
                        if (! $validators['string']($vItem)) {
                            return false;
                        }
                    }
                } elseif ($value == 'majority') {
                    return true;
                }
                return false;
            };
            $validators['w'] = $item;
        }
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
        if (! is_array($servers) && !$servers instanceof Traversable) {
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
     */
    protected function normalizeServer(& $server)
    {
        $host = static::$defaultServer['host'];
        $port = static::$defaultServer['port'];

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
            if (! isset($server[0]) && isset($server['host'])) {
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
