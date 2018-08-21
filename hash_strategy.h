/*************************************************************************
	> File Name: hash_strategy.h
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Thu 02 Aug 2018 05:45:23 PM CST
 ************************************************************************/

#define HASH_WINDOW_LEN  4
#define HASH_CALC_THREAD_NUM 64

#define HASH_MODE_SEEK 1
#define HASH_MODE_END 2

#define HASH_RECORD_WINDOW 4
struct hash_calc_result {
	struct hash_calc_result *next;
	int window_id;
	int result;
}

struct hash_calc_proc {
	int alloc_id;
	int max_item;
	struct hash_calc_result *next;
	struct hash_calc_result hresult[0];
}

struct hash_window_state {
	char *pdata;
	unsigned int real_pos;
}
