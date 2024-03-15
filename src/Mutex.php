<?php

namespace Illuminated\Console;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Redis as RedisFacade;
use NinjaMutex\Lock\FlockLock;
use NinjaMutex\Lock\LockAbstract;
use NinjaMutex\Lock\MemcachedLock;
use NinjaMutex\Lock\MySQLPDOLock;
use NinjaMutex\Lock\PhpRedisLock;
use NinjaMutex\Lock\PredisRedisLock;
use NinjaMutex\Mutex as NinjaMutex;

/**
 * @mixin \NinjaMutex\Mutex
 */
class Mutex
{
    /**
     * The console command.
     */
    private Command $command;

    /**
     * The NinjaMutex.
     */
    private NinjaMutex $ninjaMutex;

    /**
     * The NinjaMutex lock.
     */
    private LockAbstract $ninjaMutexLock;

    /**
     * Create a new instance of the mutex.
     */
    public function __construct(Command $command)
    {
        /** @var WithoutOverlapping $command */
        $this->command = $command;

        $mutexName = $command->getMutexName();
        $this->ninjaMutexLock = $this->getNinjaMutexLock();
        $this->ninjaMutex = new NinjaMutex($mutexName, $this->ninjaMutexLock);
    }

    /**
     * Get the NinjaMutex lock.
     */
    public function getNinjaMutexLock(): LockAbstract
    {
        if (!empty($this->ninjaMutexLock)) {
            return $this->ninjaMutexLock;
        }

        $strategy = $this->command->getMutexStrategy();
        switch ($strategy) {
            case 'mysql':
                return new MySQLPDOLock(
                    'mysql:' . implode(';', [
                        'host=' . config('database.connections.mysql.host'),
                        'port=' . config('database.connections.mysql.port', 3306),
                    ]),
                    config('database.connections.mysql.username'),
                    config('database.connections.mysql.password'),
                    config('database.connections.mysql.options')
                );

            case 'redis':
                return $this->getRedisLock(config('database.redis.client', 'phpredis'));

            case 'memcached':
                return new MemcachedLock(Cache::getStore()->getMemcached());

            case 'file':
            default:
                return new FlockLock($this->command->getMutexFileStorage());
        }
    }

    /**
     * Get the redis lock.
     */
    private function getRedisLock(string $client): LockAbstract
    {
        // use the env defined variable or 'mutexes' by default
        $redis_config_db = env('REDIS_MUTEX_CONFIG_DB', 'mutexes');

        // test if really configured the DB for mutexes or use the default connection
        $redis_connection = config('database.redis.' . $redis_config_db) ? $redis_config_db: null;

        // configure the redis connections for locking
        $redis = RedisFacade::connection($redis_connection)->client();

        return $client === 'phpredis'
            ? new PhpRedisLock($redis)
            : new PredisRedisLock($redis);
    }

    /**
     * Forward method calls to NinjaMutex.
     */
    public function __call(string $method, mixed $parameters): mixed
    {
        return call_user_func_array([$this->ninjaMutex, $method], $parameters);
    }
}
