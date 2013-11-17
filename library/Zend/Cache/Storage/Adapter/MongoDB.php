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
            $version = (string) phpversion('mongo');
            static::$extMongoDBVersion = (int) str_replace('.', '', $version);
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
        if (! $this->initialized) {
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

        return $mongoClient->selectDB($this->resourceManager->getDb($this->resourceId));
    }

    /**
     * @return \MongoCollection
     */
    protected function getMongoCollection()
    {
        $mongoCollection = $this->getMongoDatabase()
            ->selectCollection($this->resourceManager->getCollection($this->resourceId));

        $mongoCollection->ensureIndex(array('uid' => 1), array('unique' => true));
        $mongoCollection->ensureIndex(array('expire' => 1), array('expireAfterSeconds' => 0, 'sparse' => true));

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
        if (! $options instanceof MongoDBOptions) {
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
        if (! $this->options) {
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
            array('value' => true, 'expire' => true)
        );

        if ($data === null || $this->isNotExpired($data) === false) {
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
            array('uid' => true, 'value' => true, 'expire' => true)
        );

        $result = array();

        foreach ($cursor as $item) {
            if ($this->isNotExpired($item) === true) {
                $result[$item['uid']] = $item['value'];
            }
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

        $data = $mongoc->findOne(
            array('uid' => $normalizedKey),
            array('expire' => true)
        );

        if ($data === null || $this->isNotExpired($data) === false) {
            return false;
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
        $mongoc = $this->getMongoCollection();

        /** @var \MongoCursor $cursor */
        $cursor = $mongoc->find(
            array('uid' => array('$in' => $normalizedKeys)),
            array('uid' => true, 'expire' => true)
        );

        $result = array();

        foreach ($cursor as $item) {
            if ($this->isNotExpired($item) === true) {
                $result[] = $item['uid'];
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

        $criteria = array('uid' => $normalizedKey);
        $options = array('upsert' => true);

        $data = array(
            'uid' => $normalizedKey,
            'value' => $value,
            'mtime' => new \MongoDate(time()),
        );

        $ttl = (int) $this->getOptions()->getTtl();
        $data['ttl'] = $ttl;

        if ($ttl > 0) {
            $data['expire'] = new \MongoDate(time() + $ttl);
        }

        $result = $mongoc->update($criteria, $data, $options);

        if (true !== $result) {
            $this->checkResult($result);
        }

        return true;
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
        $mongoc = $this->getMongoCollection();

        $options = array();

        $data = array(
            'uid' => $normalizedKey,
            'value' => $value,
            'mtime' => new \MongoDate(time()),
        );

        $ttl = (int) $this->getOptions()->getTtl();
        $data['ttl'] = $ttl;

        if ($ttl > 0) {
            $data['expire'] = new \MongoDate(time() + $ttl);
        }

        try {
            $result = $mongoc->insert($data, $options);
        } catch (\MongoCursorException $exception) {
            return false;
        }

        if (true !== $result) {
            $this->checkResult($result);
        }

        return true;
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

            if ($result['n'] == 0) {
                return false;
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
        $notRemovedKeys = array();

        foreach ($normalizedKeys as $normalizedKey) {
            $result = $this->internalRemoveItem($normalizedKey);

            if ($result !== true) {
                $notRemovedKeys[] = $normalizedKey;
            }
        }

        return $notRemovedKeys;
    }

    /**
     * Internal method to get metadata of an item.
     *
     * @param  string $normalizedKey
     * @return array|bool Metadata on success, false on failure
     * @throws Exception\ExceptionInterface
     */
    protected function internalGetMetadata(& $normalizedKey)
    {
        $mongoc = $this->getMongoCollection();

        $data = $mongoc->findOne(
            array('uid' => $normalizedKey),
            array('_id' => true, 'mtime' => true, 'ttl' => true)
        );

        if ($data === null || $this->isNotExpired($data) === false) {
            return false;
        }

        /** @var \MongoId $mongoId */
        $mongoId = $data['_id'];

        $result = array(
            'internal_key' => $mongoId,
            'ctime'        => $mongoId->getTimestamp(),
            'mtime'        => $data['mtime']->sec,
            'ttl'          => $data['ttl'],
        );

        return $result;
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
                        'object'   => 'array',
                        'resource' => false,
                    ),
                    'supportedMetadata' => array(
                        'internal_key',
                        'ctime',
                        'mtime',
                        'ttl',
                    ),
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
     * Check that a cached value is not expired
     *
     * @param array $value
     * @return bool
     */
    protected function isNotExpired(array $value)
    {
        if (array_key_exists('expire', $value) && $value['expire'] instanceof \MongoDate) {
            /** @var \MongoDate $expireDate */
            $expireDate = $value['expire'];

            // expireDate is in the past -> item is expired
            if ($expireDate->sec < time()) {
                return false;
            }
        }

        // expire date is _not_ in the past or is not set at all -> item is still valid
        return true;
    }

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
