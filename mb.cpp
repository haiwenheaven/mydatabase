//ͷ�ļ� 
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h> 

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
	int file_descriptior;//�ļ��ı�ʶ�� 
	unsigned int file_length;//�ļ��ĳ��� 
	void* pages[table_max_pages];//һҳ����ʼ��ַ
}pager;
 
//table��أ�table�洢pages���飬pages�����ÿһҳ����ʼ��ַ 
const unsigned int page_size=4096;//һҳ���ֽ��� 
const unsigned int rows_per_page=page_size/row_size;//ÿһҳ������ 
const unsigned int table_max_rows=rows_per_page*table_max_pages;//ÿ�ű������ 
typedef struct{
	unsigned int num_rows;//��¼Ŀǰ���ű������
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
		free_table(t);
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

//ͨ�������ҵ��ڴ��еĵ�n+1�е���ʼλ��,���ص�ַ 
void* row_slot(int n,table* t)
{
	//������һ������һҳ�������һҳΪ�գ�������ڴ� 
	unsigned int pagenum=n/rows_per_page;
	if(t->pages[pagenum]==NULL)
	{
		t->pages[pagenum]=malloc(page_size);
	}
	//��������һҳ����һ�У���������һ�е���ʼ��ַ 
	unsigned int rownum_in_this_page=n%rows_per_page;
	unsigned int byte=rownum_in_this_page*row_size;
	return t->pages[pagenum]+byte;
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
	table *t=new_table();	
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
