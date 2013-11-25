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

    /**
     * Set up.
     */
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

        $this->setUpHostAndPortOptions();

        if (defined('TESTS_ZEND_CACHE_MONGODB_DATABASE')) {
            $this->_options->getResourceManager()
                ->setDb(
                    __CLASS__,
                    TESTS_ZEND_CACHE_MONGODB_DATABASE
                );
        }

        $this->setUpCredentialOptions();

        $this->_storage = new Cache\Storage\Adapter\MongoDB();

        $this->_storage->setOptions($this->_options);
        $this->_storage->flush();
        parent::setUp();
    }

    /**
     * Tear down.
     */
    public function tearDown()
    {
        if ($this->_storage) {
            $this->_storage->flush();
        }

        parent::tearDown();
    }

    /*
     * Host and Port Option.
     */
    protected function setUpHostAndPortOptions()
    {
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
    }

    /*
     * Username and Passwort Option.
     */
    protected function setUpCredentialOptions()
    {
        if (defined('TESTS_ZEND_CACHE_MONGODB_ENABLED') && TESTS_ZEND_CACHE_MONGODB_ENABLED) {
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
        }
    }

    /* MongoDB Storage */

    /**
     * @expectedException \MongoConnectionException
     */
    public function testUsernamePasswordFailsWithInvalidCredentials()
    {
        $user = 'foo';
        $this->_options->getResourceManager()->setUsername($this->_options->getResourceId(), $user);

        $pass = 'wrong';
        $this->_options->getResourceManager()->setPassword($this->_options->getResourceId(), $pass);

        $key = 'key';
        $this->_storage->setItem($key, '123.12');
    }

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

    public function testSocketConnectionSuccessCase()
    {
        $servers = array('/tmp/mongodb-27017.sock');
        $this->_options->getResourceManager()->setServers($this->_options->getResourceId(), $servers);

        $key = 'key';
        $this->assertTrue($this->_storage->setItem($key, 'foobar'));
        $this->assertEquals(
            'foobar',
            $this->_storage->getItem($key),
            'Problem with storing / retrieving via socket connection'
        );
    }


    /* ResourceManager */

    public function testGetSetSocketConnection()
    {
        $servers = array('/tmp/mongodb-27017.sock');
        $this->_options->getResourceManager()->setServers($this->_options->getResourceId(), $servers);
        $normalized = $this->_options->getResourceManager()->getServers($this->_options->getResourceId());
        $normalized = array_values($normalized)[0];
        $this->assertEquals(
            '/tmp/mongodb-27017.sock',
            $normalized['socket'],
            'Host should be 127.0.0.1 on socket connection'
        );

        $this->_storage = null;
    }

    public function testGetSetDatabase()
    {
        $this->assertTrue($this->_storage->setItem('key', 'val'));
        $this->assertEquals('val', $this->_storage->getItem('key'));

        $databaseName = 'zfcache_unittest2';
        $resourceManager = $this->_options->getResourceManager();
        $resourceManager->setDb($this->_options->getResourceId(), $databaseName);
        $this->assertNull(
            $this->_storage->getItem('key'),
            'No value should be found because set was done on different database than get'
        );
        $this->assertEquals(
            $databaseName,
            $resourceManager->getDb($this->_options->getResourceId()),
            'Incorrect database was returned'
        );
    }

    public function testGetSetUsernameAndPassword()
    {
        $user = 'fizz';
        $this->_options->getResourceManager()->setUsername($this->_options->getResourceId(), $user);
        $this->assertEquals(
            $user,
            $this->_options->getResourceManager()->getUsername($this->_options->getResourceId()),
            'Username was not correctly set'
        );

        $pass = 'buzz';
        $this->_options->getResourceManager()->setPassword($this->_options->getResourceId(), $pass);
        $this->assertEquals(
            $pass,
            $this->_options->getResourceManager()->getPassword($this->_options->getResourceId()),
            'Password was not correctly set'
        );
    }

    public function testGetSetOthers()
    {
        $this->markTestIncomplete("Incomplete by FooBar");
    }


    /* MongoDBOptions */

    public function testGetSetResourceManager()
    {
        $resourceManager = new Cache\Storage\Adapter\MongoDBResourceManager();
        $options = new Cache\Storage\Adapter\MongoDBOptions();
        $options->setResourceManager($resourceManager);
        $this->assertInstanceOf(
            'Zend\\Cache\\Storage\\Adapter\\MongoDBResourceManager',
            $options->getResourceManager(),
            'Wrong resource manager retuned, it should of type MongoDBResourceManager'
        );

        $this->assertEquals($resourceManager, $options->getResourceManager());
    }

    public function testGetSetResourceId()
    {
        $resourceId = '1';
        $options = new Cache\Storage\Adapter\MongoDBOptions();
        $options->setResourceId($resourceId);
        $this->assertEquals(
            $resourceId,
            $options->getResourceId(),
            'Resource id was not set correctly through MongoDBOptions'
        );
    }

    public function testGetSetServers()
    {
        $servers[] = array(
            'host' => '127.0.0.1',
            'port' => 27017,
        );
        $this->_options->setServers($servers);

        $actual = $this->_options->getServers();
        $this->assertCount(1, $actual, 'Servers count differs from what was set');
        $actual = array_values($actual)[0];
        $this->assertEquals($servers[0]['host'], $actual['host'], 'Server host differs from what was set');
        $this->assertEquals($servers[0]['port'], $actual['port'], 'Server port differs from what was set');
    }

    public function testOptionsGetSetDatabase()
    {
        $database = 'zfcache_unittest2';
        $this->_options->setDb($database);
        $this->assertEquals($database, $this->_options->getDb(), 'Database not set correctly through MongoDBOptions');
    }

    public function testGetSetNamespace()
    {
        $namespace = 'zfcache_unittest2';
        $this->_options->setNamespace($namespace);
        $this->assertEquals(
            $namespace,
            $this->_options->getNamespace(),
            'Namespace was not set correctly through MongoDBOptions'
        );
    }

    public function testOptionsGetSetUsernameAndPassword()
    {
        $username = 'fizz';
        $this->_options->setUsername($username);
        $this->assertEquals(
            $username,
            $this->_options->getUsername(),
            'Username was set incorrectly through MongoDBOptions'
        );

        $password = 'buzz';
        $this->_options->setPassword($password);
        $this->assertEquals(
            $password,
            $this->_options->getPassword(),
            'Password was set incorrectly through MongoDBOptions'
        );
    }

    public function testGetSetOtherOptions()
    {
        $this->markTestIncomplete("Incomplete by FooBar");
    }
}
