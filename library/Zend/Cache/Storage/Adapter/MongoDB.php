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
use stdClass;
use Traversable;
use Zend\Cache\Exception;
use Zend\Cache\Storage\Capabilities;
use Zend\Cache\Storage\FlushableInterface;

class MongoDB extends AbstractAdapter implements FlushableInterface
{
    /**
     * Major version of ext/mongo
     *
     * @var null|int
     */
    protected static $extMongoDBVersion;

    /**
     * Has this instance be initialized
     *
     * @var bool
     */
    protected $initialized = false;

    /**
     * The memcached resource manager
     *
     * @var null|MemcachedResourceManager
     */
    protected $resourceManager;

    /**
     * The memcached resource id
     *
     * @var null|string
     */
    protected $resourceId;

    /**
     * The namespace prefix
     *
     * @var string
     */
    protected $namespacePrefix = '';

    /**
     * Constructor
     *
     * @param  null|array|Traversable|MemcachedOptions $options
     * @throws Exception\ExceptionInterface
     */
    public function __construct($options = null)
    {
        if (static::$extMongoDBVersion === null) {
            $v = (string) phpversion('mongo');
            static::$extMongoDBVersion = (int) str_replace('.', '', $v);
        }

        if (static::$extMongoDBVersion < 141) {
            throw new Exception\ExtensionNotLoadedException('Need ext/mongodb version >= 1.4.1');
        }

        parent::__construct($options);

        // reset initialized flag on update option(s)
        $initialized = & $this->initialized;
        $this->getEventManager()->attach('option', function ($event) use (& $initialized) {
                $initialized = false;
            });
    }

    /**
     * Initialize the internal mongodb resource
     *
     * @return MongoDBResource
     */
    protected function getMongoDBResource()
    {
        if (!$this->initialized) {
            $options = $this->getOptions();

            // get resource manager and resource id
            $this->resourceManager = $options->getResourceManager();
            $this->resourceId      = $options->getResourceId();

            // init namespace prefix
            $namespace = $options->getNamespace();

            if ($namespace !== '') {
                $this->namespacePrefix = $namespace;
            } else {
                $this->namespacePrefix = '';
            }

            // update initialized flag
            $this->initialized = true;
        }

        return $this->resourceManager->getResource($this->resourceId);
    }

    /* options */

    /**
     * Set options.
     *
     * @param  array|Traversable|MongoDBOptions $options
     * @return MongoDB
     * @see    getOptions()
     */
    public function setOptions($options)
    {
        if (!$options instanceof MongoDBOptions) {
            $options = new MongoDBOptions($options);
        }

        return parent::setOptions($options);
    }

    /**
     * Get options.
     *
     * @return MongoDBOptions
     * @see setOptions()
     */
    public function getOptions()
    {
        if (!$this->options) {
            $this->setOptions(new MongoDBOptions());
        }

        return $this->options;
    }

    /* FlushableInterface */

    /**
     * Flush the whole storage
     *
     * @return bool
     * @throws
     */
    public function flush()
    {
        $mongoc = $this->getMongoDBResource();
        
        if (! $mongoc->flush()) {
            throw $this->getExceptionByResultCode($mongoc->getResultCode());
        }
        return true;
    }

    /* reading */

    /**
     * Internal method to get an item.
     *
     * @param  string  $normalizedKey
     * @param  bool $success
     * @param  mixed   $casToken
     * @return mixed Data on success, null on failure
     * @throws Exception\ExceptionInterface
     */
    protected function internalGetItem(& $normalizedKey, & $success = null, & $casToken = null)
    {
        $mongoc        = $this->getMongoDBResource();
        $internalKey = $this->namespacePrefix . $normalizedKey;

        if (func_num_args() > 2) {
            $result = $mongoc->get($internalKey, null, $casToken);
        } else {
            $result = $mongoc->get($internalKey);
        }

        $success = true;
        if ($result === false || $result === null) {
            $rsCode = $mongoc->getResultCode();
            if ($rsCode == MongoDBResource::RES_NOTFOUND) {
                $result = null;
                $success = false;
            } elseif ($rsCode) {
                $success = false;
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return $result;
    }

    /**
     * Internal method to get multiple items.
     *
     * @param  array $normalizedKeys
     * @return array Associative array of keys and values
     * @throws Exception\ExceptionInterface
     */
    protected function internalGetItems(array & $normalizedKeys)
    {
        $mongoc = $this->getMongoDBResource();

        foreach ($normalizedKeys as & $normalizedKey) {
            $normalizedKey = $this->namespacePrefix . $normalizedKey;
        }

        $result = $mongoc->getMulti($normalizedKeys);
        if ($result === false) {
            throw $this->getExceptionByResultCode($mongoc->getResultCode());
        }

        // remove namespace prefix from result
        if ($result && $this->namespacePrefix !== '') {
            $tmp            = array();
            $nsPrefixLength = strlen($this->namespacePrefix);
            foreach ($result as $internalKey => & $value) {
                $tmp[substr($internalKey, $nsPrefixLength)] = & $value;
            }
            $result = $tmp;
        }

        return $result;
    }

    /**
     * Internal method to test if an item exists.
     *
     * @param  string $normalizedKey
     * @return bool
     * @throws Exception\ExceptionInterface
     */
    protected function internalHasItem(& $normalizedKey)
    {
        $mongoc  = $this->getMongoDBResource();
        $value = $mongoc->get($this->namespacePrefix . $normalizedKey);
        if ($value === false || $value === null) {
            $rsCode = $mongoc->getResultCode();
            if ($rsCode == MongoDBResource::RES_SUCCESS) {
                return true;
            } elseif ($rsCode == MongoDBResource::RES_NOTFOUND) {
                return false;
            } else {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return true;
    }

    /**
     * Internal method to test multiple items.
     *
     * @param  array $normalizedKeys
     * @return array Array of found keys
     * @throws Exception\ExceptionInterface
     */
    protected function internalHasItems(array & $normalizedKeys)
    {
        $mongoc = $this->getMongoDBResource();

        foreach ($normalizedKeys as & $normalizedKey) {
            $normalizedKey = $this->namespacePrefix . $normalizedKey;
        }

        $result = $mongoc->getMulti($normalizedKeys);
        if ($result === false) {
            throw $this->getExceptionByResultCode($mongoc->getResultCode());
        }

        // Convert to a simgle list
        $result = array_keys($result);

        // remove namespace prefix
        if ($result && $this->namespacePrefix !== '') {
            $nsPrefixLength = strlen($this->namespacePrefix);
            foreach ($result as & $internalKey) {
                $internalKey = substr($internalKey, $nsPrefixLength);
            }
        }

        return $result;
    }

    /**
     * Get metadata of multiple items
     *
     * @param  array $normalizedKeys
     * @return array Associative array of keys and metadata
     * @throws Exception\ExceptionInterface
     */
    protected function internalGetMetadatas(array & $normalizedKeys)
    {
        $mongoc = $this->getMongoDBResource();

        foreach ($normalizedKeys as & $normalizedKey) {
            $normalizedKey = $this->namespacePrefix . $normalizedKey;
        }

        $result = $mongoc->getMulti($normalizedKeys);
        if ($result === false) {
            throw $this->getExceptionByResultCode($mongoc->getResultCode());
        }

        // remove namespace prefix and use an empty array as metadata
        if ($this->namespacePrefix !== '') {
            $tmp            = array();
            $nsPrefixLength = strlen($this->namespacePrefix);
            foreach (array_keys($result) as $internalKey) {
                $tmp[substr($internalKey, $nsPrefixLength)] = array();
            }
            $result = $tmp;
        } else {
            foreach ($result as & $value) {
                $value = array();
            }
        }

        return $result;
    }

    /* writing */

    /**
     * Internal method to store an item.
     *
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return bool
     * @throws Exception\ExceptionInterface
     */
    protected function internalSetItem(& $normalizedKey, & $value)
    {
        $mongoc       = $this->getMongoDBResource();
        $expiration = $this->expirationTime();
        if (!$mongoc->set($this->namespacePrefix . $normalizedKey, $value, $expiration)) {
            throw $this->getExceptionByResultCode($mongoc->getResultCode());
        }

        return true;
    }

    /**
     * Internal method to store multiple items.
     *
     * @param  array $normalizedKeyValuePairs
     * @return array Array of not stored keys
     * @throws Exception\ExceptionInterface
     */
    protected function internalSetItems(array & $normalizedKeyValuePairs)
    {
        $mongoc       = $this->getMongoDBResource();
        $expiration = $this->expirationTime();

        $namespacedKeyValuePairs = array();
        foreach ($normalizedKeyValuePairs as $normalizedKey => & $value) {
            $namespacedKeyValuePairs[$this->namespacePrefix . $normalizedKey] = & $value;
        }

        if (!$mongoc->setMulti($namespacedKeyValuePairs, $expiration)) {
            throw $this->getExceptionByResultCode($mongoc->getResultCode());
        }

        return array();
    }

    /**
     * Add an item.
     *
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return bool
     * @throws Exception\ExceptionInterface
     */
    protected function internalAddItem(& $normalizedKey, & $value)
    {
        $mongoc       = $this->getMongoDBResource();
        $expiration = $this->expirationTime();
        if (!$mongoc->add($this->namespacePrefix . $normalizedKey, $value, $expiration)) {
            if ($mongoc->getResultCode() == MongoDBResource::RES_NOTSTORED) {
                return false;
            }
            throw $this->getExceptionByResultCode($mongoc->getResultCode());
        }

        return true;
    }

    /**
     * Internal method to replace an existing item.
     *
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return bool
     * @throws Exception\ExceptionInterface
     */
    protected function internalReplaceItem(& $normalizedKey, & $value)
    {
        $mongoc       = $this->getMongoDBResource();
        $expiration = $this->expirationTime();
        if (!$mongoc->replace($this->namespacePrefix . $normalizedKey, $value, $expiration)) {
            $rsCode = $mongoc->getResultCode();
            if ($rsCode == MongoDBResource::RES_NOTSTORED) {
                return false;
            }
            throw $this->getExceptionByResultCode($rsCode);
        }

        return true;
    }

    /**
     * Internal method to set an item only if token matches
     *
     * @param  mixed  $token
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return bool
     * @throws Exception\ExceptionInterface
     * @see    getItem()
     * @see    setItem()
     */
    protected function internalCheckAndSetItem(& $token, & $normalizedKey, & $value)
    {
        $mongoc       = $this->getMongoDBResource();
        $expiration = $this->expirationTime();
        $result     = $mongoc->cas($token, $this->namespacePrefix . $normalizedKey, $value, $expiration);

        if ($result === false) {
            $rsCode = $mongoc->getResultCode();
            if ($rsCode !== 0 && $rsCode != MongoDBResource::RES_DATA_EXISTS) {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }


        return $result;
    }

    /**
     * Internal method to remove an item.
     *
     * @param  string $normalizedKey
     * @return bool
     * @throws Exception\ExceptionInterface
     */
    protected function internalRemoveItem(& $normalizedKey)
    {
        $mongoc   = $this->getMongoDBResource();
        $result = $mongoc->delete($this->namespacePrefix . $normalizedKey);

        if ($result === false) {
            $rsCode = $mongoc->getResultCode();
            if ($rsCode == MongoDBResource::RES_NOTFOUND) {
                return false;
            } elseif ($rsCode != MongoDBResource::RES_SUCCESS) {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return true;
    }

    /**
     * Internal method to remove multiple items.
     *
     * @param  array $normalizedKeys
     * @return array Array of not removed keys
     * @throws Exception\ExceptionInterface
     */
    protected function internalRemoveItems(array & $normalizedKeys)
    {
        // support for removing multiple items at once has been added in ext/memcached-2.0.0
        if (static::$extMongoDBVersion < 2) {
            return parent::internalRemoveItems($normalizedKeys);
        }

        $mongoc = $this->getMongoDBResource();

        foreach ($normalizedKeys as & $normalizedKey) {
            $normalizedKey = $this->namespacePrefix . $normalizedKey;
        }

        $rsCodes = $mongoc->deleteMulti($normalizedKeys);

        $missingKeys = array();
        foreach ($rsCodes as $key => $rsCode) {
            if ($rsCode !== true && $rsCode != MongoDBResource::RES_SUCCESS) {
                if ($rsCode != MongoDBResource::RES_NOTFOUND) {
                    throw $this->getExceptionByResultCode($rsCode);
                }
                $missingKeys[] = $key;
            }
        }

        // remove namespace prefix
        if ($missingKeys && $this->namespacePrefix !== '') {
            $nsPrefixLength = strlen($this->namespacePrefix);
            foreach ($missingKeys as & $missingKey) {
                $missingKey = substr($missingKey, $nsPrefixLength);
            }
        }

        return $missingKeys;
    }

    /**
     * Internal method to increment an item.
     *
     * @param  string $normalizedKey
     * @param  int    $value
     * @return int|bool The new value on success, false on failure
     * @throws Exception\ExceptionInterface
     */
    protected function internalIncrementItem(& $normalizedKey, & $value)
    {
        $mongoc        = $this->getMongoDBResource();
        $internalKey = $this->namespacePrefix . $normalizedKey;
        $value       = (int) $value;
        $newValue    = $mongoc->increment($internalKey, $value);

        if ($newValue === false) {
            $rsCode = $mongoc->getResultCode();

            // initial value
            if ($rsCode == MongoDBResource::RES_NOTFOUND) {
                $newValue = $value;
                $mongoc->add($internalKey, $newValue, $this->expirationTime());
                $rsCode = $mongoc->getResultCode();
            }

            if ($rsCode) {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return $newValue;
    }

    /**
     * Internal method to decrement an item.
     *
     * @param  string $normalizedKey
     * @param  int    $value
     * @return int|bool The new value on success, false on failure
     * @throws Exception\ExceptionInterface
     */
    protected function internalDecrementItem(& $normalizedKey, & $value)
    {
        $mongoc        = $this->getMongoDBResource();
        $internalKey = $this->namespacePrefix . $normalizedKey;
        $value       = (int) $value;
        $newValue    = $mongoc->decrement($internalKey, $value);

        if ($newValue === false) {
            $rsCode = $mongoc->getResultCode();

            // initial value
            if ($rsCode == MongoDBResource::RES_NOTFOUND) {
                $newValue = -$value;
                $mongoc->add($internalKey, $newValue, $this->expirationTime());
                $rsCode = $mongoc->getResultCode();
            }

            if ($rsCode) {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return $newValue;
    }

    /* status */

    /**
     * Internal method to get capabilities of this adapter
     *
     * @return Capabilities
     */
    protected function internalGetCapabilities()
    {
        if ($this->capabilities === null) {
            $this->capabilityMarker = new stdClass();
            $this->capabilities     = new Capabilities(
                $this,
                $this->capabilityMarker,
                array(
                    'supportedDatatypes' => array(
                        'NULL'     => true,
                        'boolean'  => true,
                        'integer'  => true,
                        'double'   => true,
                        'string'   => true,
                        'array'    => true,
                        'object'   => 'object',
                        'resource' => false,
                    ),
                    'supportedMetadata'  => array(),
                    'minTtl'             => 1,
                    'maxTtl'             => 0,
                    'staticTtl'          => true,
                    'ttlPrecision'       => 1,
                    'useRequestTime'     => false,
                    'expiredRead'        => false,
                    'maxKeyLength'       => 255,
                    'namespaceIsPrefix'  => true,
                )
            );
        }

        return $this->capabilities;
    }

    /* internal */

    /**
     * Get expiration time by ttl
     *
     * Some storage commands involve sending an expiration value (relative to
     * an item or to an operation requested by the client) to the server. In
     * all such cases, the actual value sent may either be Unix time (number of
     * seconds since January 1, 1970, as an integer), or a number of seconds
     * starting from current time. In the latter case, this number of seconds
     * may not exceed 60*60*24*30 (number of seconds in 30 days); if the
     * expiration value is larger than that, the server will consider it to be
     * real Unix time value rather than an offset from current time.
     *
     * @return int
     */
    protected function expirationTime()
    {
        $ttl = $this->getOptions()->getTtl();
        if ($ttl > 2592000) {
            return time() + $ttl;
        }
        return $ttl;
    }

    /**
     * Generate exception based of memcached result code
     *
     * @param int $code
     * @return Exception\RuntimeException
     * @throws Exception\InvalidArgumentException On success code
     */
    protected function getExceptionByResultCode($code)
    {
        switch ($code) {
            case MongoDBResource::RES_SUCCESS:
                throw new Exception\InvalidArgumentException(
                    "The result code '{$code}' (SUCCESS) isn't an error"
                );

            default:
                return new Exception\RuntimeException($this->getMongoDBResource()->getResultMessage());
        }
    }
}
