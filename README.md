/**************************************************/
/* Author: RUI YANG				  */
/* Contact: rui.yang@aalto.fi 			  */
/* Department: Aalto University School of Science */
/* Update Date: 2015-06-17 				  */
/**************************************************/
$Usage
	1. use "sh compileApp.sh" to compile the application
	2. use "sh one.sh RUI_first_configuration.txt" to run the simulator
	3. configurations for the application is maintained in RUI_first_configuration.txt and the configurations for the simulation scenario is maintained in default_settings.txt
	4. routing strategy called "EPpSWRouter" added: using epidemic in query while using SprayAndWait in response

$Done
	$1. implemented static cache and opportunaty cache(randomly generated and the content may be modified afterwards if the node got some intesting response from other nodes). By setting myapp.avaCache to enable the caches in the node;
	$2. reporter added 	

$To Do List
so many stuff
	$1. routing optimization
	$2. get and set functions in class file

	

$Observation
	$1. If the size of the buffer of node is too small, it will drop packages. This is an important issue when epidemic routing is used.
	$2. For ONE simulator to create a group of nodes, it will get the setting once from class constructor with "Setting s" as its parameter. And later, it will call the same amount of time as the num of nodes to create nodes using class constructor with "Application a" as its parameter. So be careful don't copy any unique property of one node to another in class constructor.
	

/******************* end of file ******************/
