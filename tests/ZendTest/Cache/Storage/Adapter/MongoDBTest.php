<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace ZendTest\Cache\Storage\Adapter;

use Zend\Cache;
use MongoClient as MongoDBResource;

class MongoDBTest extends CommonAdapterTest
{
    /**
     * @var Cache\Storage\Adapter\MongoDBOptions
     */
    protected $_options;

    /**
     *
     * @var Cache\Storage\Adapter\MongoDB
     */
    protected $_storage;

    public function setUp()
    {
        if (! defined('TESTS_ZEND_CACHE_MONGODB_ENABLED') || !TESTS_ZEND_CACHE_MONGODB_ENABLED) {
            $this->markTestSkipped("Skipped by TestConfiguration (TESTS_ZEND_CACHE_MONGODB_ENABLED)");
        }

        if (! extension_loaded('mongo')) {
            $this->markTestSkipped("MongoDB extension is not loaded");
        }

        $this->_options  = new Cache\Storage\Adapter\MongoDBOptions(array(
            'resource_id' => __CLASS__,
            'namespace'   => defined('TESTS_ZEND_CACHE_MONGODB_NAMESPACE')
                    ? TESTS_ZEND_CACHE_MONGODB_NAMESPACE
                    : 'zfcache'
        ));

        if (defined('TESTS_ZEND_CACHE_MONGODB_HOST') && defined('TESTS_ZEND_CACHE_MONGODB_PORT')) {
            $this->_options->getResourceManager()
                ->setServers(
                    __CLASS__,
                    array('host' => TESTS_ZEND_CACHE_MONGODB_HOST, 'port' => TESTS_ZEND_CACHE_MONGODB_PORT)
                );
        } elseif (defined('TESTS_ZEND_CACHE_MONGODB_HOST')) {
            $this->_options->getResourceManager()
                ->setServers(
                    __CLASS__,
                    array('host' => TESTS_ZEND_CACHE_MONGODB_HOST)
                );
        }

        if (defined('TESTS_ZEND_CACHE_MONGODB_DATABASE')) {
            $this->_options->getResourceManager()
                ->setDb(
                    __CLASS__,
                    TESTS_ZEND_CACHE_MONGODB_DATABASE
                );
        }

        if (defined('TESTS_ZEND_CACHE_MONGODB_USERNAME') && defined('TESTS_ZEND_CACHE_MONGODB_PASSWORD')) {
            $this->_options->getResourceManager()
                ->setUsername(
                    __CLASS__,
                    TESTS_ZEND_CACHE_MONGODB_USERNAME
                );
            $this->_options->getResourceManager()
                ->setPassword(
                    __CLASS__,
                    TESTS_ZEND_CACHE_MONGODB_PASSWORD
                );
        }

        $this->_storage = new Cache\Storage\Adapter\MongoDB();

        $this->_storage->setOptions($this->_options);
        $this->_storage->flush();
        parent::setUp();
    }

    public function tearDown()
    {
        if ($this->_storage) {
            $this->_storage->flush();
        }

        parent::tearDown();
    }

    /* MongoDB Storage */

    public function testMongoCacheStoreSuccessCase()
    {
        $key = 'singleKey';
        //assure that there's nothing under key
        $this->_storage->removeItem($key);
        $this->assertNull($this->_storage->getItem($key));
        $this->_storage->setItem($key, serialize(array('test', array('one', 'two'))));
        $actual = unserialize($this->_storage->getItem($key));

        $this->assertCount(2, $actual, 'Get item should return array of two elements');

        $expectedVals = array(
            'key1' => 'val1',
            'key2' => 'val2',
            'key3' => array('val3', 'val4'),
        );

        $this->_storage->setItems($expectedVals);

        $this->assertCount(
            3,
            $this->_storage->getItems(array_keys($expectedVals)),
            'Multiple set/get items didnt save correct amount of rows'
        );
    }

    public function testMongoSerializerSuccessCase()
    {
        $this->_storage->addPlugin(new \Zend\Cache\Storage\Plugin\Serializer());
        $value = (object) array('foo' => 'bar');
        $this->_storage->setItem('key', $value);

        $this->assertEquals($value, $this->_storage->getItem('key'), 'Problem with serialization');
    }

    public function testMongoStorageGetSetStringSuccessCase()
    {
        $key = 'key';
        $this->assertTrue($this->_storage->setItem($key, '123.12'));
        $this->assertEquals('123.12', $this->_storage->getItem($key), 'Problem with storing / retreiving a string');
    }

    public function testMongoStorageGetSetIntSuccessCase()
    {
        $key = 'key';
        $this->assertTrue($this->_storage->setItem($key, 123));
        $this->assertEquals(123, $this->_storage->getItem($key), 'Problem with storing / retreiving an integer');
    }

    public function testMongoStorageGetSetDoubleSuccessCase()
    {
        $key = 'key';
        $this->assertTrue($this->_storage->setItem($key, 123.12));
        $this->assertEquals(123.12, $this->_storage->getItem($key), 'Problem with storing / retreiving a double');
    }

    public function testMongoStorageGetSetNullSuccessCase()
    {
        $key = 'key';
        $this->assertTrue($this->_storage->setItem($key, null));
        $this->assertEquals(null, $this->_storage->getItem($key), 'Problem with storing / retreiving a null value');
    }

    public function testMongoStorageGetSetBooleanSuccessCase()
    {
        $key = 'key';
        $this->assertTrue($this->_storage->setItem($key, true));
        $this->assertEquals(true, $this->_storage->getItem($key), 'Problem with storing / retreiving a boolean true');
        $this->assertTrue($this->_storage->setItem($key, false));
        $this->assertEquals(false, $this->_storage->getItem($key), 'Problem with storing / retreiving a boolean false');
    }

    public function testMongoStorageGetSetArraySuccessCase()
    {
        $key = 'key';
        $value = array('foo', 'bar', 'a_foo' => 'a-bar');
        $this->assertTrue($this->_storage->setItem($key, $value));
        $this->assertEquals($value, $this->_storage->getItem($key), 'Problem with storing / retreiving an array');
    }

    public function testMongoStorageGetCapabilitiesSuccessCase()
    {
        $reflObject = new \ReflectionObject($this->_storage);
        $reflProp = $reflObject->getProperty('capabilities');
        $reflProp->setAccessible(true);
        $reflProp->setValue($this->_storage, null);

        $capabilities = $this->_storage->getCapabilities();
        $this->assertInstanceOf('Zend\Cache\Storage\Capabilities', $capabilities, 'Problem getting capabilities');

        $capabilities = $reflProp->getValue($this->_storage);
        $this->assertInstanceOf('Zend\Cache\Storage\Capabilities', $capabilities, 'Capabilities property was not set');
    }

    /* ResourceManager */

    public function testSocketConnection()
    {
        $socket = '/tmp/redis.sock';
        $this->_options->getResourceManager()->setServer($this->_options->getResourceId(), $socket);
        $normalized = $this->_options->getResourceManager()->getServer($this->_options->getResourceId());
        $this->assertEquals($socket, $normalized['host'], 'Host should equal to socket {$socket}');

        $this->_storage = null;
    }

    public function testGetSetDatabase()
    {
        $this->assertTrue($this->_storage->setItem('key', 'val'));
        $this->assertEquals('val', $this->_storage->getItem('key'));

        $databaseNumber = 1;
        $resourceManager = $this->_options->getResourceManager();
        $resourceManager->setDatabase($this->_options->getResourceId(), $databaseNumber);
        $this->assertNull($this->_storage->getItem('key'), 'No value should be found because set was done on different database than get');
        $this->assertEquals($databaseNumber, $resourceManager->getDatabase($this->_options->getResourceId()), 'Incorrect database was returned');
    }

    public function testGetSetPassword()
    {
        $pass = 'super secret';
        $this->_options->getResourceManager()->setPassword($this->_options->getResourceId(), $pass);
        $this->assertEquals(
            $pass,
            $this->_options->getResourceManager()->getPassword($this->_options->getResourceId()),
            'Password was not correctly set'
        );
    }

    public function testGetSetLibOptionsOnExistingRedisResourceInstance()
    {
        $options = array('serializer', RedisResource::SERIALIZER_PHP);
        $this->_options->setLibOptions($options);

        $value  = array('value');
        $key    = 'key';
        //test if it's still possible to set/get item and if lib serializer works
        $this->_storage->setItem($key, $value);
        $this->assertEquals($value, $this->_storage->getItem($key), 'Redis should return an array, lib options were not set correctly');


        $options = array('serializer', RedisResource::SERIALIZER_NONE);
        $this->_options->setLibOptions($options);
        $this->_storage->setItem($key, $value);
        //should not serialize array correctly
        $this->assertFalse(is_array($this->_storage->getItem($key)), 'Redis should not serialize automatically anymore, lib options were not set correctly');
    }

    public function testGetSetLibOptionsWithCleanRedisResourceInstance()
    {
        $options = array('serializer', RedisResource::SERIALIZER_PHP);
        $this->_options->setLibOptions($options);

        $redis = new Cache\Storage\Adapter\Redis($this->_options);
        $value  = array('value');
        $key    = 'key';
        //test if it's still possible to set/get item and if lib serializer works
        $redis->setItem($key, $value);
        $this->assertEquals($value, $redis->getItem($key), 'Redis should return an array, lib options were not set correctly');


        $options = array('serializer', RedisResource::SERIALIZER_NONE);
        $this->_options->setLibOptions($options);
        $redis->setItem($key, $value);
        //should not serialize array correctly
        $this->assertFalse(is_array($redis->getItem($key)), 'Redis should not serialize automatically anymore, lib options were not set correctly');
    }

    /* RedisOptions */

    public function testGetSetNamespace()
    {
        $namespace = 'testNamespace';
        $this->_options->setNamespace($namespace);
        $this->assertEquals($namespace, $this->_options->getNamespace(), 'Namespace was not set correctly');
    }

    public function testGetSetNamespaceSeparator()
    {
        $separator = '/';
        $this->_options->setNamespaceSeparator($separator);
        $this->assertEquals($separator, $this->_options->getNamespaceSeparator(), 'Separator was not set correctly');
    }

    public function testGetSetResourceManager()
    {
        $resourceManager = new \Zend\Cache\Storage\Adapter\RedisResourceManager();
        $options = new \Zend\Cache\Storage\Adapter\RedisOptions();
        $options->setResourceManager($resourceManager);
        $this->assertInstanceOf(
            'Zend\\Cache\\Storage\\Adapter\\RedisResourceManager',
            $options->getResourceManager(),
            'Wrong resource manager retuned, it should of type RedisResourceManager'
        );

        $this->assertEquals($resourceManager, $options->getResourceManager());
    }

    public function testGetSetResourceId()
    {
        $resourceId = '1';
        $options = new \Zend\Cache\Storage\Adapter\RedisOptions();
        $options->setResourceId($resourceId);
        $this->assertEquals($resourceId, $options->getResourceId(), 'Resource id was not set correctly');
    }

    public function testGetSetPersistentId()
    {
        $persistentId = '1';
        $this->_options->setPersistentId($persistentId);
        $this->assertEquals($persistentId, $this->_options->getPersistentId(), 'Persistent id was not set correctly');
    }

    public function testOptionsGetSetLibOptions()
    {
        $options = array('serializer', RedisResource::SERIALIZER_PHP);
        $this->_options->setLibOptions($options);
        $this->assertEquals($options, $this->_options->getLibOptions(), 'Lib Options were not set correctly through RedisOptions');
    }

    public function testGetSetServer()
    {
        $server = array(
            'host' => '127.0.0.1',
            'port' => 6379,
            'timeout' => 0,
        );
        $this->_options->setServer($server);
        $this->assertEquals($server, $this->_options->getServer(), 'Server was not set correctly through RedisOptions');
    }

    public function testOptionsGetSetDatabase()
    {
        $database = 1;
        $this->_options->setDatabase($database);
        $this->assertEquals($database, $this->_options->getDatabase(), 'Database not set correctly using RedisOptions');
    }

    public function testOptionsGetSetPassword()
    {
        $password = 'my-secret';
        $this->_options->setPassword($password);
        $this->assertEquals($password, $this->_options->getPassword(), 'Password was set incorrectly using RedisOptions');
    }

}
