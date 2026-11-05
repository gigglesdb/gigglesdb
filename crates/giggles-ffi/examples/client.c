#include "giggles.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static char *call(gd_handle h,const char *json,uint32_t code) {
    gd_handle b=0;size_t n=0;char *out;
    assert(gd_v1_call(h,(const uint8_t*)json,strlen(json),&b)==code);
    assert(b!=0);assert(gd_v1_buffer_copy(b,NULL,0,&n)==20);
    out=(char*)malloc(n+1);assert(out);assert(gd_v1_buffer_copy(b,(uint8_t*)out,n,&n)==0);out[n]=0;
    assert(gd_v1_close(b)==0);assert(gd_v1_close(b)==18);puts(out);return out;
}
static void action(gd_handle h,const char *json,uint32_t code){char *s=call(h,json,code);free(s);}
static gd_handle open_db(const char *mode,const char *path,const char *source){
    char json[4096];gd_handle h=0;
    if(source)snprintf(json,sizeof json,"{\"mode\":\"%s\",\"path\":\"%s\",\"export\":\"%s\"}",mode,path,source);
    else snprintf(json,sizeof json,"{\"mode\":\"%s\",\"path\":\"%s\"}",mode,path);
    assert(gd_v1_admin_open((const uint8_t*)json,strlen(json),&h)==0);return h;
}
int main(int argc,char **argv){
    gd_handle a,e=0,bad=99;char *p,*start,*end;char json[16384];
    const char *request="{\"operation\":1,\"selection\":{\"Keys\":[{\"tenant\":42,\"row\":0}]},\"column\":\"Status\",\"after\":\"Queued\"}";
    assert(argc==4);assert(gd_v1_abi_version()==1);
    assert(gd_v1_admin_open(NULL,1,&bad)==18);assert(bad==0);a=open_db("create",argv[1],NULL);
    action(a,"{\"command\":\"seed\",\"key\":{\"tenant\":42,\"row\":0},\"row\":{\"status\":\"Failed\",\"failed_at\":103,\"attempt_count\":0,\"version\":99}}",0);
    action(a,"{\"command\":\"grant\",\"grant\":{\"task\":1,\"issuer\":1,\"executor\":2,\"approver\":3,\"policy\":{\"tenant\":42,\"columns\":[\"Status\"],\"transitions\":[[\"Failed\",\"Queued\"]],\"incident\":[100,107],\"distinct_limit\":1,\"transition_limit\":1,\"approval_above\":0,\"valid_from\":1,\"expires\":1000}}}",0);
    assert(gd_v1_executor(a,1,2,&e)==0);action(e,"{\"command\":\"revoke\",\"task\":1}",17);
    snprintf(json,sizeof json,"{\"command\":\"preview\",\"request\":%s,\"time\":{\"Certified\":10}}",request);
    p=call(e,json,0);start=strstr(p,"\"preview\":\"");assert(start);start+=11;end=strchr(start,'"');assert(end);*end=0;
    snprintf(json,sizeof json,"{\"command\":\"approve\",\"principal\":3,\"preview\":\"%s\",\"time\":{\"Certified\":10}}",start);action(a,json,0);free(p);
    snprintf(json,sizeof json,"{\"command\":\"execute\",\"request\":%s,\"time\":{\"Certified\":10},\"approval\":0}",request);action(e,json,0);action(e,json,0);
    action(e,"{\"command\":\"status\",\"operation\":1}",0);action(e,"{\"command\":\"status\",\"operation\":99}",1);
    assert(gd_v1_executor(e,1,2,&bad)==18);assert(bad==0);
    snprintf(json,sizeof json,"{\"command\":\"export\",\"path\":\"%s\"}",argv[2]);action(a,json,0);action(a,json,13);
    assert(gd_v1_close(e)==0);assert(gd_v1_close(a)==0);
    a=open_db("open",argv[1],NULL);assert(gd_v1_executor(a,1,2,&e)==0);action(e,"{\"command\":\"status\",\"operation\":1}",0);
    action(a,"{\"command\":\"revoke\",\"task\":1}",0);
    assert(gd_v1_close(e)==0);assert(gd_v1_close(a)==0);
    a=open_db("restore",argv[3],argv[2]);assert(gd_v1_executor(a,1,2,&e)==0);action(e,"{\"command\":\"status\",\"operation\":1}",0);
    action(e,"{\"command\":\"execute\",\"request\":{\"operation\":2,\"selection\":{\"Keys\":[{\"tenant\":42,\"row\":0}]},\"column\":\"Status\",\"after\":\"Queued\"},\"time\":{\"Certified\":11},\"approval\":null}",104);
    assert(gd_v1_close(e)==0);assert(gd_v1_close(a)==0);assert(gd_v1_close(a)==18);
    puts("C client passed: approval, retry, authority, restart, export, fenced restore");return 0;
}
