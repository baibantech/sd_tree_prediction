/*************************************************************************
	> File Name: rbtree_adp.h
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Thu 13 Dec 2018 10:29:11 AM CST
 ************************************************************************/

#include "rbtree.h"
struct data_rb_tree_node {
	struct rb_node rb_node;
	int ref;
	char *data;
};
extern char *data_rb_tree_insert(char *data);
extern char *data_rb_tree_find(char *data);

