import time

import redis

from InternalConst import *
from const import *
from CustomPrint import CustomPrint

printc = 0


class Scoreboard:
    # Various functions for writing to and reading from Redis
    def __init__(self, database_num, prog_name, a_type):
        custom_print = CustomPrint(prog_name)
        custom_print.define_subname("SB")
        global printc
        printc = custom_print.printc
        printc("Setting up ConnectionPool...")
        pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=database_num)
        printc("Setting up Redis connection...")
        self._redis = redis.Redis(connection_pool=pool)
        # Scoreboard machine type, used for forwarder and distributor boards
        self._a_type = a_type
        return

    def flush_db(self):
        self._redis.flushdb()
        return

    def update_ack(self, ack_id, ack_name):
        if self._redis.sadd(ack_id, ack_name):
            return True
        return False

    def check_ack(self, ack_id, expected_acks):
        if not self._redis.smembers(ack_id):
            return False
        for name in expected_acks:
            if not self._redis.sismember(ack_id, name):
                return False
        return True

    def count_ack(self, ack_id, expected_acks):
        count = 0
        if self._redis.smembers(ack_id):
            for name in expected_acks:
                if self._redis.sismember(ack_id, name):
                    count += 1
        return count

    def missing_acks(self, ack_id, expected_acks):
        missing = list(expected_acks)
        members = self._redis.smembers(ack_id)
        if members:
            for member in members:
                missing.remove(member)
        return missing

    def machine_update(self, key, field, value):
        self._redis.hset(key, field, value)
        return

    def machine_deregister(self, name):
        self._redis.delete(name)
        self._redis.srem(self._a_type, 0, name)
        return

    def add_job(self, job_num):
        printc("Adding job %d..." % int(job_num))
        self._redis.lpush(JOBS, job_num)
        return

    def add_job_value(self, job_num, value_name, value):
        printc("Adding %s:%s for job %d" % (value_name, value, int(job_num)))
        self._redis.hset(job_num, value_name, value)
        return

    def get_job_value(self, job_num, value_name):
        return self._redis.hget(job_num, value_name)

    def set_job_state(self, job_num, state_val):
        printc("Setting job %d state: %s" % (int(job_num), state_val))
        self._redis.hset(job_num, STATE, state_val)
        return

    def get_job_state(self, job_num):
        return self._redis.hget(job_num, STATE)

    def reset_internal_job(self):
        self._redis.hset('SETTINGS', 'IJN', 0)
        return

    def new_job(self):
        internal_job_num = int(self._redis.hget('SETTINGS', 'IJN')) + 1
        self._redis.hset('SETTINGS', 'IJN', internal_job_num)
        return str(internal_job_num)

    def find_ext_job(self, ext_job_num):
        list_elements = self._redis.lrange(JOBS, 0, -1)
        for element in list_elements:
            if self._redis.hget(element, 'EXT_JOB_NUM') == str(ext_job_num) and self._redis.hget(element,
                                                                                                 'STATUS') == 'ACTIVE':
                return element
        return "NOT_FOUND"

    def count_idle(self, type):
        idle_found = 0
        num_forws = self._redis.smembers(type)
        for forw_name in num_forws:
            if self._redis.hget(forw_name, STATE) == IDLE:
                idle_found = idle_found + 1
        return idle_found

    def get_idle_list(self, type, num_needed, job_num):
        idle_found = 0
        idle_list = []
        empty_list = []
        num_forws = self._redis.smembers(type)
        for forw_name in num_forws:
            if self._redis.hget(forw_name, STATE) == IDLE:
                idle_found = idle_found + 1
                idle_list.append(forw_name)
                if idle_found == num_needed:
                    break
        if idle_found == num_needed:
            for forw_name in idle_list:
                self._redis.hset(forw_name, 'STATE', 'BUSY')
                self._redis.hset(forw_name, 'CURRENT_JOB', job_num)
            return idle_list
        else:
            return empty_list

    def machine_find_job(self, type, job_num):
        # Return list of queues for the machines whose current job is job_num
        m_list = []
        num_forws = self._redis.smembers(type)
        for forw_name in num_forws:
            if str(self._redis.hget(forw_name, 'CURRENT_JOB')) == str(job_num):
                m_list.append(self._redis.hget(forw_name, 'CONSUME_Q'))
                self._redis.hset(forw_name, 'STATE', 'IDLE')
                self._redis.hset(forw_name, 'CURRENT_JOB', "NONE")
        return m_list

    def machine_find_all_m_check(self, type, job_num):
        counter = 0
        num_dist = self._redis.smembers(type)
        for dist_name in num_dist:
            if str(self._redis.hget(dist_name, 'CURRENT_JOB')) == str(job_num):
                counter = counter + 1
        return counter

    def machine_find_all_m(self, type, job_num):
        m_list = []
        num_dist = self._redis.smembers(type)
        for dist_name in num_dist:
            if str(self._redis.hget(dist_name, 'CURRENT_JOB')) == str(job_num):
                m_list.append(dist_name)
        return m_list

    def machine_find_all_pairs(self, job_num):
        m_list = {}
        num_dist = self._redis.smembers(LIST_FORWARDERS)
        for dist_name in num_dist:
            if str(self._redis.hget(dist_name, 'CURRENT_JOB')) == str(job_num):
                m_list[dist_name] = str(self._redis.hget(str(job_num) + ":" + dist_name, 'PARTNER'))
        return m_list

    def set_list_to_idle(self, m_list):
        for m_name in m_list:
            self._redis.hset(m_name, 'STATE', 'IDLE')
            self._redis.hset(m_name, 'CURRENT_JOB', "NONE")
        return

    def get_consume_q(self, machine):
        return self._redis.hget(machine, 'CONSUME_Q')

    def change_machine_status_to_idle(self, m_name):
        self._redis.hset(m_name, 'STATE', 'IDLE')
        self._redis.hset(m_name, 'CURRENT_JOB', 'NONE')
        return

    def get_machine_job_num(self, m_name):
        return self._redis.hget(m_name, 'CURRENT_JOB')

    def get_machine_value(self, m_name, key):
        return self._redis.hget(m_name, key)

    def register_machine(self, name):
        if 1 == self._redis.sismember(self._a_type, name):
            return False
        self._redis.sadd(self._a_type, name)
        self._redis.hset(name, 'STATE', 'IDLE')
        self._redis.hset(name, 'CONSUME_Q', name + "_consume")
        if REGISTER_FORWARDER == self._a_type:
            self._redis.hset(name, 'PUBLISH_Q', Q_FORW_PUBLISH)
        elif REGISTER_DISTRIBUTOR == self._a_type:
            self._redis.hset(name, 'PUBLISH_Q', Q_DIST_PUBLISH)
        self._redis.hset(name, 'ROUTING_KEY', name + "_consume")
        return True
