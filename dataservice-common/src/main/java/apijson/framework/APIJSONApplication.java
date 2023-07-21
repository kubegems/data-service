/*Copyright ©2016 TommyLemon(https://github.com/TommyLemon/APIJSON)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package apijson.framework;

import java.rmi.ServerException;

import apijson.NotNull;

/**
 * SpringBootApplication 右键这个类 > Run As > Java Application
 * 
 * @author Lemon
 */
public class APIJSONApplication {

	@NotNull
	public static APIJSONCreator DEFAULT_APIJSON_CREATOR;
	static {
		DEFAULT_APIJSON_CREATOR = new APIJSONCreator();
	}

	/**
	 * 初始化，加载所有配置并校验
	 * 
	 * @return
	 * @throws ServerException
	 */
	public static void init() throws ServerException {
		init(true, DEFAULT_APIJSON_CREATOR);
	}

	/**
	 * 初始化，加载所有配置并校验
	 * 
	 * @param shutdownWhenServerError
	 * @return
	 * @throws ServerException
	 */
	public static void init(boolean shutdownWhenServerError) throws ServerException {
		init(shutdownWhenServerError, DEFAULT_APIJSON_CREATOR);
	}

	/**
	 * 初始化，加载所有配置并校验
	 * 
	 * @param creator
	 * @return
	 * @throws ServerException
	 */
	public static void init(@NotNull APIJSONCreator creator) throws ServerException {
		init(true, creator);
	}

	/**
	 * 初始化，加载所有配置并校验
	 * 
	 * @param shutdownWhenServerError
	 * @param creator
	 * @return
	 * @throws ServerException
	 */
	public static void init(boolean shutdownWhenServerError, @NotNull APIJSONCreator creator) throws ServerException {
		System.out.println("\n\n\n\n\n<<<<<<<<<<<<<<<<<<<<<<<<< APIJSON 开始启动 >>>>>>>>>>>>>>>>>>>>>>>>\n");
		DEFAULT_APIJSON_CREATOR = creator;
		// 统一用同一个 creator
		APIJSONSQLConfig.APIJSON_CREATOR = creator;
		APIJSONParser.APIJSON_CREATOR = creator;
		APIJSONController.APIJSON_CREATOR = creator;
	}

}
