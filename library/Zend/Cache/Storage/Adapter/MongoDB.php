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
     * @var null|MongoDBResourceManager
     */
    protected $resourceManager;

    /**
     * The memcached resource id
     *
     * @var null|string
     */
    protected $resourceId;

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

            // update initialized flag
            $this->initialized = true;
        }

        return $this->resourceManager->getResource($this->resourceId);
    }

    /**
     * @return \MongoDB
     */
    protected function getMongoDatabase()
    {
        $mongoClient = $this->getMongoDBResource();

        return $mongoClient->selectDB($this->resourceManager->getDB($this->resourceId));
    }

    /**
     * @return \MongoCollection
     */
    protected function getMongoCollection()
    {
        $mongoCollection = $this->getMongoDatabase()
            ->selectCollection($this->resourceManager->getCollection($this->resourceId));

        $mongoCollection->ensureIndex(array('uid' => 1, 'unique' => true));
        $mongoCollection->ensureIndex(array('expire' => 1, 'expireAfterSeconds' => 0));

        return $mongoCollection;
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
        $mongoc = $this->getMongoCollection();

        $result = $mongoc->remove(array());

        if (true !== $result) {
            $this->checkResult($result);
        }

        return true;
    }

    /* reading */

    /**
     * Internal method to get an item.
     *
     * @param  string $normalizedKey
     * @param  bool   $success
     * @param  mixed  $casToken
     * @return mixed Data on success, null on failure
     */
    protected function internalGetItem(& $normalizedKey, & $success = null, & $casToken = null)
    {
        $mongoc = $this->getMongoCollection();

        $data = $mongoc->findOne(
            array('uid' => $normalizedKey),
            array('value' => 1)
        );

        // TODO: check if it's possible that mongo delivers expired items (we know they still can be in the collection)

        if ($data === null) {
            $success = false;
            return null;
        }

        $success = true;
        $casToken = $data['value'];

        return $data['value'];
    }

    /**
     * Internal method to get multiple items.
     *
     * @param  array $normalizedKeys
     * @return array Associative array of keys and values
     */
    protected function internalGetItems(array & $normalizedKeys)
    {
        $mongoc = $this->getMongoCollection();

        /** @var \MongoCursor $cursor */
        $cursor = $mongoc->find(
            array('uid' => array('$in' => $normalizedKeys)),
            array('uid' => 1, 'value' => 1)
        );

        $result = array();

        foreach ($cursor as $item) {
            $result[$item['uid']] = $item['value'];
        }

        return $result;
    }

    /**
     * Internal method to test if an item exists.
     *
     * @param  string $normalizedKey
     * @return bool
     */
    protected function internalHasItem(& $normalizedKey)
    {
        $mongoc = $this->getMongoCollection();

        /** @var \MongoCursor $cursor */
        $cursor = $mongoc->find(
            array('uid' => $normalizedKey),
            array('uid' => 1)
        );

        return ($cursor->count() === 0) ? false : true;
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
        $mongoc = $this->getMongoCollection();

        /** @var \MongoCursor $cursor */
        $cursor = $mongoc->find(
            array('uid' => array('$in' => $normalizedKeys)),
            array('uid' => 1)
        );

        $result = array();

        foreach ($cursor as $item) {
            $result[] = $item['uid'];
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
        $mongoc = $this->getMongoCollection();
        $ttl = $this->getOptions()->getTtl();
        $expiration = time() + $ttl;

        $criteria = array();
        $options = array('upsert' => true);

        $data = array(
            'uid' => $normalizedKey,
            'value' => $value,
            'expire' => new \MongoDate($expiration)
        );

        $result = $mongoc->update($criteria, $data, $options);

        if (true !== $result) {
            $this->checkResult($result);
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
        foreach ($normalizedKeyValuePairs as $key => $value) {
            $this->internalSetItem($key, $value);
        }

        return array();
    }

    /**
     * Add an item.
     *
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return bool
     */
    protected function internalAddItem(& $normalizedKey, & $value)
    {
        return $this->internalSetItem($normalizedKey, $value);
    }

    /**
     * Internal method to replace an existing item.
     *
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return bool
     */
    protected function internalReplaceItem(& $normalizedKey, & $value)
    {
        return $this->internalSetItem($normalizedKey, $value);
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
        $mongoc = $this->getMongoCollection();

        $result = $mongoc->remove(array('uid' => $normalizedKey));

        if (true !== $result) {
            $this->checkResult($result);
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
        foreach ($normalizedKeys as $normalizedKey) {
            $this->internalRemoveItem($normalizedKey);
        }

        return array();
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
        // TODO: implement
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
        // TODO: implement
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
                    'namespaceIsPrefix'  => false,
                )
            );
        }

        return $this->capabilities;
    }

    /* internal */

    /**
     * Generate exception based of memcached result code
     *
     * @param array $result
     * @throws Exception\RuntimeException
     */

    protected function checkResult(array $result)
    {
        if ($result['ok'] == 0) {
            throw new Exception\RuntimeException($result['errmsg']);
        }
    }
}
