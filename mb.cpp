//头文件 
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h> 

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
	int file_descriptor;//文件的标识符 
	unsigned int file_length;//文件的长度 
	void* pages[table_max_pages];//一页的起始地址
}pager;
 
//table相关，table存储pages数组，pages数组存每一页的起始地址 
const unsigned int page_size=4096;//一页的字节数 
const unsigned int rows_per_page=page_size/row_size;//每一页的行数 
const unsigned int table_max_rows=rows_per_page*table_max_pages;//每张表的行数 
typedef struct{
	unsigned int num_rows;//记录目前这张表的行数
	pager* p;//这张表的缓冲区 
}table;

//打开这张表对应的文件并建立一个缓冲区，返回这个缓冲区
pager* pager_open(const char* filename)
{
	/*
	int open(const char* pathname,int flags,mode_t mode)
	pathname:指向欲打开文件路径字符串
	flags:旗标，表明打开文件的方式
	mode:只有在新建文件时才会生效，与文件权限有关
	返回值:若所有欲核查的权限都通过了检查则返回该文件的文件描述符，表示成功，只要有一个权限被禁止则返回-1。
	*/
	int fd=open(filename,O_RDWR | O_CREAT);
	if(fd==-1)
	{
		printf("unable to open file.\n");
		exit(EXIT_FAILURE);
	}
	/*
	off_t:用来指示文件的偏移量 
	*/
	/*
	off_t lseek(int filedes,off_t offset,int whence)
	功能:lseek()便是用来控制该文件的读写位置.
		 每一个打开的文件都有一个读写位置，当打开文件时通常其读写位置是指向文件开头
	 	 若是若是以附加的方式打开文件(如O_APPEND), 则读写位置会指向文件尾. 
	     当read()或write()时, 读写位置会随之增加。
	fileds:打开文件的描述符 
	offset:根据参数whence来移动读写位置的位移数 
	whence:
		SEEK_SET 参数offset 即为新的读写位置.
    	SEEK_CUR 以目前的读写位置往后增加offset 个位移量.
    	SEEK_END 将读写位置指向文件尾后再增加offset 个位移量. 当whence 值为SEEK_CUR 或
    	SEEK_END 时, 参数offet 允许负值的出现.
    另:fseek()用来移动文件流的读写位置 
	*/ 
	off_t file_length=lseek(fd,0,SEEK_END);
	pager* p=(pager*)malloc(sizeof(pager));
	p->file_descriptor=fd;
	p->file_length=file_length; 
	//开始时缓冲区为空 
	for (uint32_t i = 0; i < table_max_pages; i++) 
	{
   		p->pages[i] = NULL;
 	}
	 return p; 
}
//打开一个文件并为其建立一个缓冲区，缓冲区初始为空 
table* db_open(const char* filename)
{
	pager* p=pager_open(filename);//打开文件 
	unsigned int num_rows=p->file_length/row_size;//文件中的行数	
	table* t=(table*)malloc(sizeof(table));
	t->p=p;
	t->num_rows=num_rows;
	return t;
}

//把pager中第page_num页共size个字节复制到硬盘 
void pager_flush(pager *p,unsigned int page_num,unsigned int size)
{
	if(p->pages[page_num]==NULL)
	{
		printf("tried to flush null page.\n");
		exit(EXIT_FAILURE);	
	}
	off_t offset=lseek(p->file_descriptor,page_num*page_size,SEEK_SET);
	if(offset==-1)
	{
		printf("error seeking.\n");
		exit(EXIT_FAILURE);
	}
	/*
	ssize_t write(int fd,char* buffer,int size)
	功能:从buffer中写size大小的字节到文件fd中 
	*/
	ssize_t bytes_written=write(p->file_descriptor,p->pages[page_num],size);
	if(bytes_written==-1)
	{
		printf("error writing.\n");
		exit(EXIT_FAILURE);
	 } 
}

//关闭文件并把缓存中的数据复制到硬盘 
void db_close(table* t)
{
	//先把这些满页复制到硬盘并释放内存 
	unsigned int pages_full_num=t->num_rows/rows_per_page;
	for(unsigned int i=0;i<pages_full_num;i++)
	{
		if(t->p->pages[i]==NULL)//说明这一页还没有加载到内存，不用复制到硬盘 
		{
			continue;	
		}
		pager_flush(t->p,i,page_size);
		//free(t->p->pages[i]);
		//t->p->pages[i]=NULL;重复操作？ 
	}
	//最后一页可能没有满 
	if(t->num_rows%rows_per_page)
	{
		if(t->p->pages[pages_full_num]!=NULL)
		{
			pager_flush(t->p,pages_full_num,(t->num_rows%rows_per_page)*row_size);
			free(t->p->pages[pages_full_num]);
			t->p->pages[pages_full_num]=NULL;
		}
	}
	//关闭文件
	/*
	int close(int fd)
	功能:关闭一个已经打开的文件
	fd:文件描述符
	返回值:成功返回0，失败返回-1 
	*/
	int result=close(t->p->file_descriptor);
	if(result==-1)
	{
		printf("error closing db file.\n");
		exit(EXIT_FAILURE);
	}
	for(int i=0;i<table_max_pages;i++)
	{
		if(t->p->pages[i])
		{
			free(t->p->pages[i]);
			t->p->pages[i]=NULL;
		}
	}
	free(t->p);
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
		db_close(t); 
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

void* get_page(pager* p,unsigned int page_num)
{
	if(page_num>table_max_pages)//表满了 
	{
		printf("tried to fetch page number out of bounds.\n");
		exit(EXIT_FAILURE);
	}
	
	if(p->pages[page_num]==NULL)//缓存miss，从硬盘复制到内存 
	{
		p->pages[page_num]=(void*)malloc(page_size);
		unsigned int num_pages_now=p->file_length/page_size;
		if((p->file_length%page_size)>0)
		{
			num_pages_now++;
		}
		/*
		ssize_t read(int fd,void* buf,size_t count)
		fd:文件描述符 
		buf:读出数据的缓冲区
		count:读取的字节数
		返回值:实际读到的字节数，当有错误时返回-1 
		*/
		if(page_num<=num_pages_now)//当page_num没有越界时，才把硬盘中的数据复制到内存，否则直接给p->pages[page_num]分配内存就可以返回了 
		{
			lseek(p->file_descriptor,page_num*page_size,SEEK_SET);//找到这个文件这一行的起始位置 ，把读写位置设置在这里 
			ssize_t bytes_read=read(p->file_descriptor,p->pages[page_num],page_size);
			if(bytes_read==-1)
			{
				printf("error reading file.\n");
				exit(EXIT_FAILURE);
			}
		}
	}
	return p->pages[page_num];
}
//通过行数找到内存中的第n+1行的起始位置,如果miss了，则从硬盘中取，返回地址 
void* row_slot(unsigned int n,table* t)
{
	//计算这一行在哪一页 
	unsigned int page_num=n/rows_per_page;
	//如果内存中没有这一页的缓存，则硬盘中读出这一页 
	void* page=get_page(t->p,page_num);
	//计算在这一页的哪一行，并计算这一行的起始地址 
	unsigned int rownum_in_this_page=n%rows_per_page;
	unsigned int byte=rownum_in_this_page*row_size;
	return page+byte;
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
	if(argc<2)
	{
		printf("must supply a database filename.\n");
		exit(EXIT_FAILURE);
	}
	
	char* filename=argv[1];
	table *t=db_open(filename);	
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
