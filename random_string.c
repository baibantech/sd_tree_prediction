/*************************************************************************
	> File Name: random_string.c
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: 2019年07月02日 星期二 16时42分00秒
 ************************************************************************/

#include<stdio.h>
#include<stdlib.h>
#include "chunk.h"
int random_seed = 0;

int get_random_string(char *str, int len)
{
	int i, flag;
	srand(random_seed++);
	for (i = 0; i < len; i++) {
		flag = rand()%3;
		switch(flag) {
			case 0:
 				str[i] = rand()%26 + 'a';
				break;
			case 1:
				str[i] = rand()%26 + 'A';
				break;
			case 2:
				str[i] = rand()%10 +  '0';
				break;
		}
	}
	//printf("%s\r\n",str);
}
int get_string_len()
{
	srand(random_seed++);
	return rand();
}

void make_test_data_set(char *mem, int ins_len, int flag)
{
	int len1, len2,len3;

	len1 = 8 + get_string_len()%8;
	len2 = 8 + get_string_len()%8;
	len3 = 8 + get_string_len()%8;
	
	if (len1 + len2 + len3 > ins_len)
		spt_assert(0);

	mem[0] = '/';
	get_random_string(mem++, len1 - 1);
	mem = mem + len1- 1;

	mem[0] = '/';
	get_random_string(mem++, len2 - 1);
	mem = mem + len1- 1;

	mem[0] = '/';
	get_random_string(mem++, len1 - 2);
	mem = mem + len1- 2;
	mem[0] = '#';
}
