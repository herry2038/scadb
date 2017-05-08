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
package org.herry2038.scadb.scadb.conf

import org.herry2038.scadb.db.Configuration
import java.net.URI
import scala.collection.mutable.HashMap
import java.nio.charset.Charset

object UrlParse {
	def parseJdbcUrl(url:String) : Configuration = {
	    if ( ! url.startsWith("jdbc:")) {
	        throw new RuntimeException("url " + url + " is not legal!") ;
	    }
	    val uri = URI.create(url.substring(5))	    
	    val host = uri.getHost
	    val port = uri.getPort
	    val schema = uri.getScheme
	    val db = uri.getPath.substring(1)
	    
	    val arr = url.split('?')
	    if ( arr.length == 1 ) {
	        throw new RuntimeException("url " + url + " is not legal!") ;
	    }
	    
	    val properties = arr(1).split('&')
	    
	    val mapProperties = (HashMap[String,String]() /: properties) {
	        ( all, properity ) =>
	            val p = properity.split('=')
	            if ( p.length > 1) 
	            	all += ( p(0)-> p(1) )
	            else 
	                all += ( p(0)->null )
	    }
	    
	    val user = mapProperties("user")
	    val pass = mapProperties("password")
	    val characterEncoding = mapProperties.get("characterEncoding").getOrElse("UTF-8")
	    
                          
	    new Configuration(user,host,port,Some(pass),
	            Some(db),
	            Charset.forName(characterEncoding),true,false,true)
	    
	}
}
