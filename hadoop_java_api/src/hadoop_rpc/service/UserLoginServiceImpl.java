package hadoop_rpc.service;

import hadoop_rpc.protocol.IUserLoginService;

public class UserLoginServiceImpl implements IUserLoginService{

	@Override
	public String login(String name, String passwd) {
		
		return name + "logged in successfully...";
	}
	
	
	

}
