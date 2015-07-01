#Client 操作说明
请使用python2进行操作，需安装requests包

##接口说明
### `read_config(file)`
该函数可读取`conf/settings.conf`配置文件，必须在使用其它接口之前调用

### `get(), insert(), update(), delete(), dump(), countkey(), shutdown()`
以上各函数的第一个参数为`node_id`，用于区分各个服务器，其余参数与定义相同，返回值为得到的结果以及耗费时间

##操作示例
		
		from client.client_ops import *
		
		read_config()
		
		get(1, "aaa") # 向服务器1发送get请求
		
		insert(2, "aaa", "bbb")
		
		update(3, "aaa", "kkk")
		
		delete(1, "aaa")
		
		print dump()
		print countkey()
		