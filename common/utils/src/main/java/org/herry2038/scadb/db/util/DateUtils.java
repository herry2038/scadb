//=========================================================================\\
//     _____               _ _
//    / ____|             | | |
//   | (___   ___ __ _  __| | |__
//    \___ \ / __/ _` |/ _` | '_ \
//    ____) | (_| (_| | (_| | |_) |
//   |_____/ \___\__,_|\__,_|_.__/

// Copyright 2016 The Scadb Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//=========================================================================\\
package org.herry2038.scadb.db.util;


import java.util.Date;
import java.util.Locale;

public class DateUtils {

    public static String getNowTime(String format, Locale loc) {
        if (format == null) {
            format = "yyyy-MM-dd HH:mm:ss";
        }

        if (loc == null)
            return new java.text.SimpleDateFormat(format).format(java.util.Calendar.getInstance().getTime());

        return new java.text.SimpleDateFormat(format, loc).format(java.util.Calendar.getInstance().getTime());
    }

    public static String getNowTime(String format){
        return getNowTime(format, null);
    }

    /**
     * @return
     */
    public static String getNowTime() {
        return getNowTime(null);
    }

    public static String format(Date d, String format) {
        return new java.text.SimpleDateFormat(format).format(d);
    }

    public static int getYmdDate(long ts) {
        Date now = new Date(ts * 1000) ;
        return getYmdDate(now) ;
    }

    public static int getYmdDate(Date d) {
        return (d.getYear() + 1900) * 10000 + (d.getMonth() + 1) * 100 + d.getDate();
    }

    public static void main (String[] args ){
        System.out.println(DateUtils.getNowTime("yyyy-MM-dd"));
    }
}
