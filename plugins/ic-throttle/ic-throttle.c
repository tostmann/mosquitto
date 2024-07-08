/*
Copyright (c) 2024 Dirk Tostmann

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Dirk Tostmann - initial implementation and documentation.
*/

/*
 * Discard high frequent duplicates
 *
 * Compile with:
 *   rm ic-throttle.so; gcc -I../.. -I../../include -I../../deps/ -fPIC -shared ic-throttle.c -o ic-throttle.so
 *
 * Use in config with:
 *
 *   plugin /path/to/ic-throttle.so
 *   # define topics and their time to discard in seconds:
 *   plugin_opt_throttle foo/# 1
 *   plugin_opt_throttle bing/+/bong/+ 3
 *
 */
#include "config.h"

#include <stdio.h>
#include <time.h>
#include <string.h>

#include "mosquitto_broker.h"
#include "mosquitto_plugin.h"
#include "mosquitto.h"
#include "mqtt_protocol.h"

#include <openssl/evp.h>

#include "uthash.h"
#include "utlist.h"

#define MD5_STRING_SIZE 17

static mosquitto_plugin_id_t *mosq_pid = NULL;

// https://troydhanson.github.io/uthash/userguide.html#_string_keys
struct icThrottle_struct {
    const unsigned char *topic; /* key */
    unsigned char payload[MD5_STRING_SIZE];
    struct timespec ts;
    UT_hash_handle hh; /* makes this structure hashable */
};

typedef struct icThrottle_cfg_struct {
    char *topic;
    unsigned int hold;
    struct icThrottle_cfg_struct *next, *prev;
} icThrottle_cfg_struct;


void compute_md5(char *str, unsigned int strlen, unsigned char *digest) {
    EVP_MD_CTX *mdctx;
    unsigned int md5_digest_len = EVP_MD_size(EVP_md5()); // this is expected to be "16"

    memset( digest, 0, MD5_STRING_SIZE );

    // MD5_Init
    mdctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(mdctx, EVP_md5(), NULL);

    // MD5_Update
    EVP_DigestUpdate(mdctx, str, strlen);

    // MD5_Final
    EVP_DigestFinal_ex(mdctx, digest, &md5_digest_len);
    EVP_MD_CTX_free(mdctx);

    // we wont allow NULLs
    for (uint8_t i = 0; i < md5_digest_len; i++) {
	if (!digest[i]) digest[i] = i+1;
//	mosquitto_log_printf(MOSQ_LOG_DEBUG, "%x", digest[i]);
    }
    mosquitto_log_printf(MOSQ_LOG_DEBUG, " ");
    
}

int tlencmp(icThrottle_cfg_struct *a, icThrottle_cfg_struct *b) {

    int x = strlen(a->topic);
    int y = strlen(b->topic);
    
    return x - y;
}

struct icThrottle_struct     * icThrottle     = NULL;
struct icThrottle_cfg_struct * icThrottle_cfg = NULL;
struct timespec last_tick_ts;

int handle_tick(int event, void *event_data, void *user_data)
{

    struct timespec ts;
    struct icThrottle_struct *s, *tmp = NULL;
    
    clock_gettime(CLOCK_REALTIME, &ts);
    uint16_t td = ts.tv_sec-last_tick_ts.tv_sec;

    if (td<10)
	return MOSQ_ERR_SUCCESS;
    
    mosquitto_log_printf(MOSQ_LOG_DEBUG, "Housekeeping...");
    clock_gettime(CLOCK_REALTIME, &last_tick_ts);

    /* free the hash table contents */
    HASH_ITER(hh, icThrottle, s, tmp) {
	if (ts.tv_sec-s->ts.tv_sec>60) {
	    HASH_DEL(icThrottle, s);
	    mosquitto_free(s);
	    mosquitto_log_printf(MOSQ_LOG_DEBUG, "removed entry");
	}
    }

//    mosquitto_broker_publish_copy(NULL, "topic/tick", strlen("test-message"), "test-message", 0, false, NULL);
    
    return MOSQ_ERR_SUCCESS;
}


static int callback_message(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_message *ed = event_data;
    struct timespec ts;
    char time_buf[25];
    unsigned char thash[MD5_STRING_SIZE]; 
    unsigned char phash[MD5_STRING_SIZE]; 
    struct icThrottle_struct * s = NULL;
    struct icThrottle_cfg_struct * sl = NULL;
    bool    result;
    uint8_t hold = 0;
    
    UNUSED(event);
    UNUSED(userdata);

//    return MOSQ_ERR_DISCARD;

    // check need
    if (ed->qos)
	return MOSQ_ERR_SUCCESS;
    
    DL_FOREACH(icThrottle_cfg, sl) {
	mosquitto_topic_matches_sub( sl->topic, ed->topic, &result);
	if (result)
	    hold = sl->hold;
    }

    if (hold == 0)
	return MOSQ_ERR_SUCCESS;
    
    // Compute hashes:
    compute_md5(ed->topic, strlen(ed->topic), thash);
    compute_md5(ed->payload, ed->payloadlen,  phash);

    HASH_FIND_STR( icThrottle, thash, s);
	
    if (s) {
	mosquitto_log_printf(MOSQ_LOG_DEBUG, "Found Topic %s", ed->topic);

	for (uint8_t i = 0; i < EVP_MD_size(EVP_md5()); i++) {
	    if (!phash[i])
		break;
		
	    if (phash[i] != s->payload[i]) {
		// mosquitto_log_printf(MOSQ_LOG_DEBUG, "payload pos: %d - %x %x", i, phash[i], s->payload[i]);
		mosquitto_log_printf(MOSQ_LOG_DEBUG, "payload differs");

		// update payload
		memcpy( s->payload, phash, sizeof(phash) );
		clock_gettime(CLOCK_REALTIME, &(s->ts));
		return MOSQ_ERR_SUCCESS;
	    }
	}

	clock_gettime(CLOCK_REALTIME, &ts);
	uint16_t td = ts.tv_sec-s->ts.tv_sec;
	mosquitto_log_printf(MOSQ_LOG_DEBUG, "found payload! its %d sek old", td);
	if (td>hold) {
	    mosquitto_log_printf(MOSQ_LOG_DEBUG, "re-newing it");
	    clock_gettime(CLOCK_REALTIME, &(s->ts));
	    return MOSQ_ERR_SUCCESS;
	}

	mosquitto_log_printf(MOSQ_LOG_INFO, "dropping msg due to throtteling for %s", ed->topic);
	return MOSQ_ERR_DISCARD;
	    
    } else {

	// topic is new to us - create entry:
	    
	s = (struct icThrottle_struct *)mosquitto_malloc(sizeof *s);
	s->topic = mosquitto_strdup(thash);
	memcpy( s->payload, phash, sizeof(phash) );
	clock_gettime(CLOCK_REALTIME, &(s->ts));
	    
	HASH_ADD_KEYPTR(hh, icThrottle, s->topic, strlen(s->topic), s);
    }
	
    return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{

    for(uint8_t i=0; i<supported_version_count; i++)
	if(supported_versions[i] == 5)
	    return 5;
    
    return -1;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **user_data, struct mosquitto_opt *opts, int opt_count)
{
    struct icThrottle_cfg_struct * entry = NULL;

    UNUSED(user_data);
    
    for(uint8_t i=0; i<opt_count; i++){
	if(!strcasecmp(opts[i].key, "throttle")){
	    mosquitto_log_printf(MOSQ_LOG_DEBUG, "found option for throttle");

	    entry = (icThrottle_cfg_struct *)malloc(sizeof *entry);
	
	    char *c = strchr(opts[i].value, ' ');
	    if(c) {
		entry->hold = atoi( c+1 );
		*c = 0;
	    } else  {
		entry->hold  = 3;
	    }
	    entry->topic = mosquitto_strdup(opts[i].value);
	    mosquitto_log_printf(MOSQ_LOG_DEBUG, "throtteling on topic %s for %d sek", entry->topic, entry->hold);
	    DL_APPEND(icThrottle_cfg, entry);
	}
    }

    DL_SORT(icThrottle_cfg, tlencmp);

    clock_gettime(CLOCK_REALTIME, &last_tick_ts);

    // https://github.com/eclipse/mosquitto/issues/2219
    
    mosq_pid = identifier;
    mosquitto_callback_register(mosq_pid, MOSQ_EVT_TICK, handle_tick, NULL, NULL);
    return mosquitto_callback_register(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL, NULL);
}

int mosquitto_plugin_cleanup(void *user_data, struct mosquitto_opt *opts, int opt_count)
{
    UNUSED(user_data);
    UNUSED(opts);
    UNUSED(opt_count);

    struct icThrottle_struct *s, *tmp = NULL;
    struct icThrottle_cfg_struct *ls, *ltmp = NULL;


    /* now delete each element, use the safe iterator */
    DL_FOREACH_SAFE(icThrottle_cfg,ls,ltmp) {
	DL_DELETE(icThrottle_cfg,ls);
	free(ls);
    }
   
    /* free the hash table contents */
    HASH_ITER(hh, icThrottle, s, tmp) {
	HASH_DEL(icThrottle, s);
	mosquitto_free(s);
    }

    mosquitto_callback_unregister(mosq_pid, MOSQ_EVT_TICK, handle_tick, NULL);

    return mosquitto_callback_unregister(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL);
}
