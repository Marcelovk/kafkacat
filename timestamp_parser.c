#ifndef TESTME
#include "kafkacat.h"
#endif

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>

static unsigned long num_of_seconds(char character)
{
        char lc = tolower(character);
        switch (lc)
        {
        case 'd':
                return 86400;
        case 'h':
                return 3600;
        case 'm':
                return 60;
        case 's':
                return 1;
        }
        return 0;
}

static unsigned long parse_relative_timestamp_string(char *offset_parameter, long relative)
{
        //must start with +/-
        if ((offset_parameter[0] != '+') && (offset_parameter[0] != '-'))
        {
                return 0;
        }

        //must end with valid char
        char lastChar = tolower(offset_parameter[strlen(offset_parameter) - 1]);
        if ((lastChar != 'd') && (lastChar != 'h') && (lastChar != 'm') && (lastChar != 's'))
        {
                return 0;
        }

        unsigned long calculated_ts = relative;

        char *p = offset_parameter;
        char *current_parsed_value = p;

        short inverser = 1;
        while (*p != '\0')
        {
                long current_parsed_value_as_long;
                char modifier = tolower(*p);

                switch (modifier)
                {
                case '-':
                case '+':
                        inverser = (*p == '-') ? -1 : 1;
                        current_parsed_value = p + 1;
                        break;
                case 'd':
                case 'h':
                case 'm':
                case 's':
                        current_parsed_value_as_long = strtoll(current_parsed_value, NULL, 10) * num_of_seconds(*p);
                        calculated_ts += inverser * current_parsed_value_as_long;
                        current_parsed_value = p + 1;
                        break;
                }

                p++;
        }

        return calculated_ts;
}

static unsigned long parse_iso8061_date_value(char *offset_parameter)
{
        //valid formats are just the date ('2018-01-01') or 
        //the day plus time with space ('2018-01-01 12:22:22')
       
        struct tm t = {0};
        char * fmt = "%d-%d-%d %d:%d:%d";
        //short int t.year=0, day=0, month=0, hour=0, minute=0, second=0;
        int p = sscanf(offset_parameter, fmt, &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec);

        t.tm_year -= 1900;
        t.tm_mon -= 1;
        t.tm_isdst = 0;

        //avoid funky things by sscanf
        if (p == 4) t.tm_hour = 0;
        if (p == 5) t.tm_min = 0;

        if (p >= 3) {
                long partial_result = timegm(&t);
                char * minusplus = offset_parameter+10;
                while (*minusplus != '\0') {
                        if (*minusplus == '+' || *minusplus == '-') {
                                break;
                        }
                        minusplus++;
                }
                
                if (*minusplus == '\0') {
                        return partial_result;
                } else {
                        //if you have + or - you can use the modifier to say a time from that time
                        return parse_relative_timestamp_string(minusplus, partial_result);
                }
        }
        

        return 0;

}

unsigned long parse_offset_parameter(char *offset_parameter)
{
        char *p_end;
        unsigned long simple_timestamp = strtoll(offset_parameter, &p_end, 10);
        if ((simple_timestamp == EINVAL) || (*p_end != '\0'))
        {
                unsigned long iso8061date = parse_iso8061_date_value(offset_parameter);
                if (iso8061date != 0)
                {
                        return iso8061date*1000;
                }
                else
                {
                        return parse_relative_timestamp_string(offset_parameter, time(NULL))*1000;
                }
        }
        else
        {
                return simple_timestamp;
        }
}

#ifdef TESTME

int allPass = 1;

void test(char *a, long b)
{
        long res = parse_offset_parameter(a);
        printf("%s --> ", a);
        if (res == b)
        {
                printf("PASS");
        } else {
                printf("FAIL");
                allPass = 0;
        }
        printf(" expected %ld got %ld\r\n", b, res);
}

int main(int n, char **args)
{
        test("2018-12-10-1h", 1544400000000 - 3600000);
        test("2018-12-10 00:00:00+1m", 1544400060000);
        test("2018-12-10 00:01:00", 1544400060000);

        test("2018-12-10 00:01:00", 1544400060000);
        test("2018-12-10 00:00:00", 1544400000000);

        
        test("2018-12-10", 1544400000000);
        test("12345", 12345);

        test("+60m", (time(NULL) + 60 * 60) * 1000);
        test("+7d-4h", (time(NULL) + 7 * 24 * 60 * 60 - 4 * 60 * 60) * 1000);
        test("+7d+4h", (time(NULL) + 7 * 24 * 60 * 60 + 4 * 60 * 60) * 1000);

        test("7d-4h", 0);
        test("+7d-4", 0);

        test("+7s", (time(NULL) + 7) * 1000);

        if (allPass == 0) {
                printf("Some test(s) failed");
                return 1;
        }
        return 0;
}
#endif
