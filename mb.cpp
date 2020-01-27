//头文件 
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h> 

//输入缓存
#define max_buffer_length 100 
char buffer[max_buffer_length];
int input_length;

//命令类型 
typedef enum{
	STATEMENT_INSERT,
	STATEMENT_SELECT
}StatementType;

//命令执行的结果
typedef enum{
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
}ExecuteResult;

 
//元命令执行的结果 
typedef enum{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
}MetaCommandResult;

//语法分析的结果 
typedef enum{
	PREPARE_SUCCESS,
	PREPARE_SYNTAX_ERROR,
	PREPARE_STRING_TOO_LONG,
	PREPARE_NEGATIVE_ID,
	PREPARE_UNRECOGNIZED_STATEMENT
}PrepareResult;


//行的属性约束
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
//每一行的结构体
typedef struct{
	unsigned int id;
	char username[COLUMN_USERNAME_SIZE+1];
	char email[COLUMN_EMAIL_SIZE+1];
}row;
//计算各个属性所占的字节数 
#define size_of_attribute(struct,attribute) sizeof(((struct*)0)->attribute) 
const unsigned int id_size=size_of_attribute(row,id);
const unsigned int username_size=size_of_attribute(row,username);
const unsigned int email_size=size_of_attribute(row,email);
//计算每个属性在一行中开始的offset以及一行的大小 
const unsigned int id_offset=0;
const unsigned int username_offset=id_offset+id_size;
const unsigned int email_offset=username_offset+username_size;
const unsigned int row_size=id_size+username_size+email_size;

//pager相关，pager是table的缓存 
#define table_max_pages 100//一张表最多的页数 
typedef struct{
	int file_descriptior;//文件的标识符 
	unsigned int file_length;//文件的长度 
	void* pages[table_max_pages];//一页的起始地址
}pager;
 
//table相关，table存储pages数组，pages数组存每一页的起始地址 
const unsigned int page_size=4096;//一页的字节数 
const unsigned int rows_per_page=page_size/row_size;//每一页的行数 
const unsigned int table_max_rows=rows_per_page*table_max_pages;//每张表的行数 
typedef struct{
	unsigned int num_rows;//记录目前这张表的行数
	pager* p;
}table;
table* new_table()
{
	table* t=(table*)malloc(sizeof(table));
	t->num_rows=0;
	for(int i=0;i<table_max_pages;i++)
	{
		t->pages[i]=NULL;	
	}	
}
void free_table(table* t)
{
	for(int i=0;t->pages[i];i++)
	{
		free(t->pages[i]);
	}
	free(t);
}

//语句结构体 
typedef struct{
	StatementType type;
	row row_to_insert;
}Statement;


//执行元命令 
MetaCommandResult do_meta_command(table* t)
{
	if (strcmp(buffer, ".exit") == 0)
	{
		free_table(t);
    	exit(EXIT_SUCCESS);
  	}
	else
	{
    	return META_COMMAND_UNRECOGNIZED_COMMAND;
  	}
}

//输出提示符 
void print_prompt() { printf("db > "); }

//读入命令 
void read_input() {
	input_length=0;
	char c;
	while(1)
	{
		scanf("%c",&c);
		if((int)c==10)
		{
			break;
		}
		buffer[input_length]=c;
		input_length++;
	}
	buffer[input_length]='\0';
}


PrepareResult prepare_insert(Statement* s)
{
	s->type=STATEMENT_INSERT;
	/*
	strtok(char *s,char *delem)
	功能：将字符串分割成一个个片段
	第一个参数：欲分割的字符串，如果为NULL，则函数的保存的指针SAVA_PTR在下一次调用中将作为起始位置，SAVA_PTR是隐式保存的，保存了下一子串的起始位置 
	第二个参数：分割的标志，当发现第二个参数时会把它改为'\0'
	注意：在第一次调用时，strtok()必需给予参数s 字符串，往后的调用则将参数s 设置成NULL。每次调用成功则返回下一个分割后的字符串指针 
	返回值：分隔符匹配到的第一个子串的的起始地址，如果没有要分割的子串了，返回NULL 
	*/
	char* keyword=strtok(buffer," ");
	char* id_string=strtok(NULL," ");
	char* username=strtok(NULL," ");
	char* email=strtok(NULL," ");
	if(id_string==NULL || username==NULL || email==NULL)
	{
		return PREPARE_SYNTAX_ERROR;	
	}
	/*
	atoi(char* s)
	功能：将字符串转换成int，前面的空格忽略直到遇到正负号或数字
	返回值：如果不能转化成int或字符串为空，则返回0
	        字符串过大，则返回-1； 
	*/
	int id_int=atoi(id_string);
	if(id_int<=0)
	{
		return PREPARE_NEGATIVE_ID;
	}
	if(strlen(username)>COLUMN_USERNAME_SIZE)
	{
		return PREPARE_STRING_TOO_LONG;
	}
	if(strlen(email)>COLUMN_EMAIL_SIZE)
	{
		return PREPARE_STRING_TOO_LONG;
	}
	
	s->row_to_insert.id=id_int;
	strcpy(s->row_to_insert.username,username);
	strcpy(s->row_to_insert.email,email);
	
	return PREPARE_SUCCESS;
}
//语法分析，目前有insert和select,并将从语句中提取出来的参数传递给statement 
PrepareResult prepare_statement(Statement* statement)
{
	if(strncmp(buffer,"insert",6)==0)
	{
		/*
		statement->type=STATEMENT_INSERT;
		sscanf()：
		返回值：转换的参数个数
		功能：将buffer中的字符串按照第二个参数的格式输入到后面的参数中
		注意：中间的空格不会算到格式中，多出来的会被忽视掉 
		int args_assigned=sscanf(buffer,"insert %d %s %s",&(statement->row_to_insert.id),statement->row_to_insert.username, statement->row_to_insert.email);
		if(args_assigned<3)
		{
			return PREPARE_SYNTAX_ERROR;
		}
		return PREPARE_SUCCESS;
		*/
		return prepare_insert(statement);
	}
	if(strcmp(buffer,"select")==0)
	{
		statement->type=STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	return PREPARE_UNRECOGNIZED_STATEMENT;
}

//把一行放到指定的内存 
void serialize(row* source,void* destination)
{
	/*memcpy():内存拷贝函数 
	  功能：从源内存地址的起始位置开始拷贝若干个字节到目标内存地址中
	   第一个参数：目的地址
	   第二个参数：源地址
	   第三个参数：拷贝的字节数 
	   返回：一个指向目标存储区的指针 
	*/
	memcpy(destination+id_offset,&(source->id),id_size);
	memcpy(destination+username_offset,&(source->username),username_size);
	memcpy(destination+email_offset,&(source->email),email_size);
}

//把指定内存的一行放入结构体 
void deserialize(void* source,row* destination)
{
	memcpy(&(destination->id),source+id_offset,id_size);
	memcpy(&(destination->username),source+username_offset,username_size);
	memcpy(&(destination->email),source+email_offset,email_size);
}

//通过行数找到内存中的第n+1行的起始位置,返回地址 
void* row_slot(int n,table* t)
{
	//计算这一行在哪一页，如果这一页为空，则分配内存 
	unsigned int pagenum=n/rows_per_page;
	if(t->pages[pagenum]==NULL)
	{
		t->pages[pagenum]=malloc(page_size);
	}
	//计算在这一页的哪一行，并计算这一行的起始地址 
	unsigned int rownum_in_this_page=n%rows_per_page;
	unsigned int byte=rownum_in_this_page*row_size;
	return t->pages[pagenum]+byte;
}


//执行插入操作 
ExecuteResult execute_insert(Statement *s,table* t)
{
	if(t->num_rows>=table_max_rows)
	{
		return EXECUTE_TABLE_FULL;
	}
	row* r=&(s->row_to_insert);
	serialize(r,row_slot(t->num_rows,t));
	t->num_rows++;
	return EXECUTE_SUCCESS;
}


//执行选择操作 
void print_row(row* r)
{
	printf("(%d,%s,%s)\n",r->id,r->username,r->email);
}
ExecuteResult execute_select(Statement *s,table* t)
{
	row r;
	for(int i=0;i<t->num_rows;i++)
	{
		deserialize(row_slot(i,t),&r);
		print_row(&r);
	}
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* s,table* t)
{
	switch(s->type)
	{
		case(STATEMENT_INSERT):
		{
			return execute_insert(s,t);
		}
		case(STATEMENT_SELECT):
		{
			return execute_select(s,t);
		}
	}
 }

int main(int argc, char* argv[])
{
	table *t=new_table();	
	while (true) 
	{ 
		print_prompt();
    	read_input();
    	
		//检查是否是元命令 
    	if (buffer[0]== '.') 
    	{
			switch(do_meta_command(t))
      		{
      			case (META_COMMAND_SUCCESS):
      			{
      				continue;//这个continue是continuewhile（TRUE）	
				}
				case (META_COMMAND_UNRECOGNIZED_COMMAND):
				{
					printf("unrecognized command\n");
					continue;//同理 
				}
	  	}
	}
	
	//语法分析 
	Statement statement;
	switch(prepare_statement(&statement))
	{
		
	 	case (PREPARE_SUCCESS):
			{
				break;//这个break是终止switch 
		 	}
		case (PREPARE_STRING_TOO_LONG):
			{
				printf("string is too long.\n");
				continue;
			}
		case (PREPARE_NEGATIVE_ID):
			{
				printf("id must be positive.\n");
				continue;
			}
		case (PREPARE_SYNTAX_ERROR):
			{
				printf("syntax error.\n");
				continue;
			}
		case (PREPARE_UNRECOGNIZED_STATEMENT):
			{
				printf("unrecognized keyword\n");
				continue;//这个continue是继续while 
			}
		  
  	}
  	
  	//执行 
  	switch(execute_statement(&statement,t))
  	{
  		case (EXECUTE_SUCCESS):
		   {
		  		printf("executed.\n");
		  		break;
		   }
		case (EXECUTE_TABLE_FULL):
			{
				printf("error:table full.\n");
				break;			
			}
	} 
  } 
}
