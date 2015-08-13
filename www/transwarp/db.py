#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2015-08-13 15:59:21
# @Author  : Happiness Wang (w564263331@gmail.com)
# @Link    : 
# @Version : $Id$

'''

Database operation module
'''

import time,uuid,functools,threading,logging

class Dict(dict):
	"""docstring for Dict"""
	def __init__(self, names = (),values = (),**kw):
		super(Dict, self).__init__(**kw)
		for k,v in zip(names,values):
			self[k] = v

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError (r"'Dict' object has no attribute '%s'" % key)

	def __setatter__(self,key,value):
		self[key] = value
		

def next_id(t = None):
	if t is None:
		t = time.time()
	return '%015d%s000' % (int(t * 1000),uuid.uuid4().hex)

def _profiling(t = None):
	if t is None:
		t = time.time()
	return '%015d%s000' % (int(t * 1000),uuid.uuid4().hex)
	pass

class DBError(Exception):
	"""DB Error"""
	pass

class MulticolumnsError(DBError):
	"""docstring for MulticolumnsError"""
	pass


class _LasyConnection(object):
	"""docstring for _LasyConnection"""
	def __init__(self):
		self.connection = None

	def cursor(self):
		if self.connection is None:
			connection = engine.connect()
			logging.info('open connection <%s>' % hex(id(connection)))
			self.connection = connection
		return self.connection.cursor()

	def commit(self):
		self.connection.commit()
	
	def rollback(self):
		self.connection.rollback()
	
	def cleanup(self):
		if self.connection:
			connection = self.connection
			self.connection = None
			logging.info('close connection <%s>...' % hex(id(connection)))
			connection.close()
		

class _Engine(object):
 	"""数据库引擎类"""
 	def __init__(self, connect):
 		self.connect = connect

 	def connect(self):
 		return	self.connect

engine = None

def create_engine(user,password,database,host='127.0.0.1',port = 3306,**kw):
	import mysql.connector
	global engine
	if engine is not None:
		raise DBError('Engine is already initialized')

	params = dict(user = user,password = password,database = database,port = port,host = host)
	defaults = dict(use_unicode = True,charset = 'utf-8',collation = 'utf_general_ci',autocommit = False)
	for K,v in defaults.iteritems():
		params[k] = kw.pop(k,v)

	params.update(kw)
	params['buffered'] = True
	engine = _Engine(lambda: mysql.connector.connect(**params))
	logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

class _DbCtx(threading.local):
	"""持有数据库连接的上下文对象"""
	def __init__(self):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		return not self.connection is None

	def init(self):
		pass

	def cleanup(self):
		self.connection.cleanup()
		self.transactions = 0

	def cursor(self):
		return self.connection.cursor()

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
	"""docstring for _ConnectionCtx"""
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True
			return	self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def connection():
	return	_ConnectionCtx()

def with_connection(func):
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _ConnectionCtx():
			return func(*args,**kw)
	return _wrapper


class _TransactionCtx(object):
	"""docstring for _TransactionCtx"""
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_ctx.transactions + 1
		return self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions - 1
		try:
			if _db_ctx.transactions == 0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		try:
			_db_ctx.connection.commit()
		except Exception, e:
			_db_ctx.connection.rollback()
			raise
	def rollback(self):
		global _db_ctx
		_db_ctx.connection.rollback()
		
def transaction():
	return _TransactionCtx()

def with_transtion(func):

	@functools.wraps(func)
	def _wrapper(*args,**kw):
		_start = time.time()
		with _TransactionCtx():
			return func(*args,**kw)
		_profiling(_start)
	return _wrapper
	pass

def _select(sql,first,*args):
	global _db_ctx
	cursor = None
	sql = sql.replace('?','%s')
	logging.info('SQL:%s,ARGS:%s' % (sql,args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql,args)
		if cursor.description:
			names = [x[0] for x in cursor.description]

		if first:
			values =cursor.fetchone()
			if not values:
				return None
			return Dict(names,values)

		return[Dict(names,x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close()

@with_connection
def select_one(sql,*args):

	return _select(sql,True,args)

@with_connection
def select_int(sql ,*args):
	d = _select(sql,True,*args)
	if len(d) != 1:
		raise MulticolumnsError('Expect only one column')
	return d.values()[0]

@with_connection
def select(sql, *args):
    return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
	global _db_ctx
	cursor = None
	sql = sql.replace('?', '%s')
	logging.info('SQL:%s,ARGS:%s' % (sql,args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql, args)
		r = cursor.rowcount
		if _db_ctx.transactions==0:
			# no transaction enviroment:
			logging.info('auto commit')
			_db_ctx.connection.commit()
		return r
	finally:
		if cursor:
			cursor.close()

def update(sql, *args):
    return _update(sql, *args)

if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	create_engine('happiness','root','test')
	update('drap table if exists user')
	update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
	import doctest
	doctest.testmod()