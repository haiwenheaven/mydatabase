//ͷ�ļ� 
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h> 

//���뻺��
#define max_buffer_length 100 
char buffer[max_buffer_length];
int input_length;

//�������� 
typedef enum{
	STATEMENT_INSERT,
	STATEMENT_SELECT
}StatementType;

//����ִ�еĽ��
typedef enum{
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
}ExecuteResult;

 
//Ԫ����ִ�еĽ�� 
typedef enum{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
}MetaCommandResult;

//�﷨�����Ľ�� 
typedef enum{
	PREPARE_SUCCESS,
	PREPARE_SYNTAX_ERROR,
	PREPARE_STRING_TOO_LONG,
	PREPARE_NEGATIVE_ID,
	PREPARE_UNRECOGNIZED_STATEMENT
}PrepareResult;


//�е�����Լ��
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
//ÿһ�еĽṹ��
typedef struct{
	unsigned int id;
	char username[COLUMN_USERNAME_SIZE+1];
	char email[COLUMN_EMAIL_SIZE+1];
}row;
//�������������ռ���ֽ��� 
#define size_of_attribute(struct,attribute) sizeof(((struct*)0)->attribute) 
const unsigned int id_size=size_of_attribute(row,id);
const unsigned int username_size=size_of_attribute(row,username);
const unsigned int email_size=size_of_attribute(row,email);
//����ÿ��������һ���п�ʼ��offset�Լ�һ�еĴ�С 
const unsigned int id_offset=0;
const unsigned int username_offset=id_offset+id_size;
const unsigned int email_offset=username_offset+username_size;
const unsigned int row_size=id_size+username_size+email_size;

//pager��أ�pager��table�Ļ��� 
#define table_max_pages 100//һ�ű�����ҳ�� 
typedef struct{
	int file_descriptor;//�ļ��ı�ʶ�� 
	unsigned int file_length;//�ļ��ĳ��� 
	void* pages[table_max_pages];//һҳ����ʼ��ַ
}pager;
 
//table��أ�table�洢pages���飬pages�����ÿһҳ����ʼ��ַ 
const unsigned int page_size=4096;//һҳ���ֽ��� 
const unsigned int rows_per_page=page_size/row_size;//ÿһҳ������ 
const unsigned int table_max_rows=rows_per_page*table_max_pages;//ÿ�ű������ 
typedef struct{
	unsigned int num_rows;//��¼Ŀǰ���ű������
	pager* p;//���ű�Ļ����� 
}table;

//�����ű��Ӧ���ļ�������һ�����������������������
pager* pager_open(const char* filename)
{
	/*
	int open(const char* pathname,int flags,mode_t mode)
	pathname:ָ�������ļ�·���ַ���
	flags:��꣬�������ļ��ķ�ʽ
	mode:ֻ�����½��ļ�ʱ�Ż���Ч�����ļ�Ȩ���й�
	����ֵ:���������˲��Ȩ�޶�ͨ���˼���򷵻ظ��ļ����ļ�����������ʾ�ɹ���ֻҪ��һ��Ȩ�ޱ���ֹ�򷵻�-1��
	*/
	int fd=open(filename,O_RDWR | O_CREAT);
	if(fd==-1)
	{
		printf("unable to open file.\n");
		exit(EXIT_FAILURE);
	}
	/*
	off_t:����ָʾ�ļ���ƫ���� 
	*/
	/*
	off_t lseek(int filedes,off_t offset,int whence)
	����:lseek()�����������Ƹ��ļ��Ķ�дλ��.
		 ÿһ���򿪵��ļ�����һ����дλ�ã������ļ�ʱͨ�����дλ����ָ���ļ���ͷ
	 	 ���������Ը��ӵķ�ʽ���ļ�(��O_APPEND), ���дλ�û�ָ���ļ�β. 
	     ��read()��write()ʱ, ��дλ�û���֮���ӡ�
	fileds:���ļ��������� 
	offset:���ݲ���whence���ƶ���дλ�õ�λ���� 
	whence:
		SEEK_SET ����offset ��Ϊ�µĶ�дλ��.
    	SEEK_CUR ��Ŀǰ�Ķ�дλ����������offset ��λ����.
    	SEEK_END ����дλ��ָ���ļ�β��������offset ��λ����. ��whence ֵΪSEEK_CUR ��
    	SEEK_END ʱ, ����offet ����ֵ�ĳ���.
    ��:fseek()�����ƶ��ļ����Ķ�дλ�� 
	*/ 
	off_t file_length=lseek(fd,0,SEEK_END);
	pager* p=(pager*)malloc(sizeof(pager));
	p->file_descriptor=fd;
	p->file_length=file_length; 
	//��ʼʱ������Ϊ�� 
	for (uint32_t i = 0; i < table_max_pages; i++) 
	{
   		p->pages[i] = NULL;
 	}
	 return p; 
}
//��һ���ļ���Ϊ�佨��һ������������������ʼΪ�� 
table* db_open(const char* filename)
{
	pager* p=pager_open(filename);//���ļ� 
	unsigned int num_rows=p->file_length/row_size;//�ļ��е�����	
	table* t=(table*)malloc(sizeof(table));
	t->p=p;
	t->num_rows=num_rows;
	return t;
}

//��pager�е�page_numҳ��size���ֽڸ��Ƶ�Ӳ�� 
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
	����:��buffer��дsize��С���ֽڵ��ļ�fd�� 
	*/
	ssize_t bytes_written=write(p->file_descriptor,p->pages[page_num],size);
	if(bytes_written==-1)
	{
		printf("error writing.\n");
		exit(EXIT_FAILURE);
	 } 
}

//�ر��ļ����ѻ����е����ݸ��Ƶ�Ӳ�� 
void db_close(table* t)
{
	//�Ȱ���Щ��ҳ���Ƶ�Ӳ�̲��ͷ��ڴ� 
	unsigned int pages_full_num=t->num_rows/rows_per_page;
	for(unsigned int i=0;i<pages_full_num;i++)
	{
		if(t->p->pages[i]==NULL)//˵����һҳ��û�м��ص��ڴ棬���ø��Ƶ�Ӳ�� 
		{
			continue;	
		}
		pager_flush(t->p,i,page_size);
		//free(t->p->pages[i]);
		//t->p->pages[i]=NULL;�ظ������� 
	}
	//���һҳ����û���� 
	if(t->num_rows%rows_per_page)
	{
		if(t->p->pages[pages_full_num]!=NULL)
		{
			pager_flush(t->p,pages_full_num,(t->num_rows%rows_per_page)*row_size);
			free(t->p->pages[pages_full_num]);
			t->p->pages[pages_full_num]=NULL;
		}
	}
	//�ر��ļ�
	/*
	int close(int fd)
	����:�ر�һ���Ѿ��򿪵��ļ�
	fd:�ļ�������
	����ֵ:�ɹ�����0��ʧ�ܷ���-1 
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

//���ṹ�� 
typedef struct{
	StatementType type;
	row row_to_insert;
}Statement;


//ִ��Ԫ���� 
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

//�����ʾ�� 
void print_prompt() { printf("db > "); }

//�������� 
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
	���ܣ����ַ����ָ��һ����Ƭ��
	��һ�����������ָ���ַ��������ΪNULL�������ı����ָ��SAVA_PTR����һ�ε����н���Ϊ��ʼλ�ã�SAVA_PTR����ʽ����ģ���������һ�Ӵ�����ʼλ�� 
	�ڶ����������ָ�ı�־�������ֵڶ�������ʱ�������Ϊ'\0'
	ע�⣺�ڵ�һ�ε���ʱ��strtok()����������s �ַ���������ĵ����򽫲���s ���ó�NULL��ÿ�ε��óɹ��򷵻���һ���ָ����ַ���ָ�� 
	����ֵ���ָ���ƥ�䵽�ĵ�һ���Ӵ��ĵ���ʼ��ַ�����û��Ҫ�ָ���Ӵ��ˣ�����NULL 
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
	���ܣ����ַ���ת����int��ǰ��Ŀո����ֱ�����������Ż�����
	����ֵ���������ת����int���ַ���Ϊ�գ��򷵻�0
	        �ַ��������򷵻�-1�� 
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
//�﷨������Ŀǰ��insert��select,�������������ȡ�����Ĳ������ݸ�statement 
PrepareResult prepare_statement(Statement* statement)
{
	if(strncmp(buffer,"insert",6)==0)
	{
		/*
		statement->type=STATEMENT_INSERT;
		sscanf()��
		����ֵ��ת���Ĳ�������
		���ܣ���buffer�е��ַ������յڶ��������ĸ�ʽ���뵽����Ĳ�����
		ע�⣺�м�Ŀո񲻻��㵽��ʽ�У�������Ļᱻ���ӵ� 
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

//��һ�зŵ�ָ�����ڴ� 
void serialize(row* source,void* destination)
{
	/*memcpy():�ڴ濽������ 
	  ���ܣ���Դ�ڴ��ַ����ʼλ�ÿ�ʼ�������ɸ��ֽڵ�Ŀ���ڴ��ַ��
	   ��һ��������Ŀ�ĵ�ַ
	   �ڶ���������Դ��ַ
	   �������������������ֽ��� 
	   ���أ�һ��ָ��Ŀ��洢����ָ�� 
	*/
	memcpy(destination+id_offset,&(source->id),id_size);
	memcpy(destination+username_offset,&(source->username),username_size);
	memcpy(destination+email_offset,&(source->email),email_size);
}

//��ָ���ڴ��һ�з���ṹ�� 
void deserialize(void* source,row* destination)
{
	memcpy(&(destination->id),source+id_offset,id_size);
	memcpy(&(destination->username),source+username_offset,username_size);
	memcpy(&(destination->email),source+email_offset,email_size);
}

void* get_page(pager* p,unsigned int page_num)
{
	if(page_num>table_max_pages)//������ 
	{
		printf("tried to fetch page number out of bounds.\n");
		exit(EXIT_FAILURE);
	}
	
	if(p->pages[page_num]==NULL)//����miss����Ӳ�̸��Ƶ��ڴ� 
	{
		p->pages[page_num]=(void*)malloc(page_size);
		unsigned int num_pages_now=p->file_length/page_size;
		if((p->file_length%page_size)>0)
		{
			num_pages_now++;
		}
		/*
		ssize_t read(int fd,void* buf,size_t count)
		fd:�ļ������� 
		buf:�������ݵĻ�����
		count:��ȡ���ֽ���
		����ֵ:ʵ�ʶ������ֽ��������д���ʱ����-1 
		*/
		if(page_num<=num_pages_now)//��page_numû��Խ��ʱ���Ű�Ӳ���е����ݸ��Ƶ��ڴ棬����ֱ�Ӹ�p->pages[page_num]�����ڴ�Ϳ��Է����� 
		{
			lseek(p->file_descriptor,page_num*page_size,SEEK_SET);//�ҵ�����ļ���һ�е���ʼλ�� ���Ѷ�дλ������������ 
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
//ͨ�������ҵ��ڴ��еĵ�n+1�е���ʼλ��,���miss�ˣ����Ӳ����ȡ�����ص�ַ 
void* row_slot(unsigned int n,table* t)
{
	//������һ������һҳ 
	unsigned int page_num=n/rows_per_page;
	//����ڴ���û����һҳ�Ļ��棬��Ӳ���ж�����һҳ 
	void* page=get_page(t->p,page_num);
	//��������һҳ����һ�У���������һ�е���ʼ��ַ 
	unsigned int rownum_in_this_page=n%rows_per_page;
	unsigned int byte=rownum_in_this_page*row_size;
	return page+byte;
}


//ִ�в������ 
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


//ִ��ѡ����� 
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
    	
		//����Ƿ���Ԫ���� 
    	if (buffer[0]== '.') 
    	{
			switch(do_meta_command(t))
      		{
      			case (META_COMMAND_SUCCESS):
      			{
      				continue;//���continue��continuewhile��TRUE��	
				}
				case (META_COMMAND_UNRECOGNIZED_COMMAND):
				{
					printf("unrecognized command\n");
					continue;//ͬ�� 
				}
	  	}
	}
	
	//�﷨���� 
	Statement statement;
	switch(prepare_statement(&statement))
	{
		
	 	case (PREPARE_SUCCESS):
			{
				break;//���break����ֹswitch 
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
				continue;//���continue�Ǽ���while 
			}
		  
  	}
  	
  	//ִ�� 
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
