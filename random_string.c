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
char *random_seg1[16];
char *random_seg2[16];
int random_seg_len1;
int random_seg_len2;
int random_seg_len3;

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
	return 0;
}
int get_string_len()
{
	srand(random_seed++);
	return rand();
}

int get_string_bit_len(char *str, unsigned int startbit)
{
	unsigned int startlen = startbit/8;
	unsigned int begin_off = startbit % 8;
	char *start_byte = NULL;
	int bit_len = 0;
	start_byte = str + startlen;
	
	bit_len = 8 - begin_off;
	
	if (*start_byte == '#') {
		return bit_len;
	}
	if (startbit > 8) {
		if (*(start_byte - 1) == '#')
			return 0;
	}

	start_byte++;
	while (*start_byte != '#') {
		bit_len += 8;
		start_byte++;
	}
	bit_len += 8;
	return bit_len;
}
int random_string_cnt;
void make_test_data_set(char *mem, int ins_len, int flag)
{
	int len1, len2,len3;
	if (random_string_cnt %100)
		flag = 1;
	else
		flag = 0;
	random_string_cnt++;

	if (flag == 0) {
		random_seg_len1 = len1 = 8 + get_string_len()%8;
		random_seg_len2 = len2 = 8 + get_string_len()%8;
		random_seg_len3 = len3 = 8 + get_string_len()%8;
		
		if (len1 + len2 + len3 > ins_len)
			spt_assert(0);

		get_random_string(mem, len1);
		mem[0] = '/';
		memcpy(random_seg1,mem, len1);
		mem = mem + len1;


		get_random_string(mem, len2);
		mem[0] = '/';
		memcpy(random_seg2, mem, len2);
		mem = mem + len2;

		get_random_string(mem, len3);
		mem[0] = '/';
		mem = mem + len3- 1;
		mem[0] = '#';

	} else {
		memcpy(mem, random_seg1, random_seg_len1);
		mem = mem + random_seg_len1;

		memcpy(mem, random_seg2, random_seg_len2);
		mem = mem + random_seg_len2;
		
		get_random_string(mem, random_seg_len3);
		mem[0] = '/';
		mem = mem + random_seg_len3- 1;
		mem[0] = '#';
	}
}
