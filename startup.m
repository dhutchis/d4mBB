% This file is for installing D4M
% Use it with as per the instructions in the README of D4M
  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  D4M_HOME = '/home/accuser/software/d4m_api';       % SET TO LOCATION OF D4M.

  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  addpath([D4M_HOME '/matlab_src']);            % Add the D4M library.

  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  Assoc('','','');                              % Initialize library.

  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  % Uncomment the following line to enable the D4M database connector.
  DBinit;    % This requires that the libext/ directory is in place.

  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  % Uncomment and modify the following four lines for parallel D4M.
  %PMATLAB_HOME = '/home/accuser/SVN/pMatlab';   % SET location of pMatlab.
  %addpath([PMATLAB_HOME '/MatlabMPI/src']);    % Add MatlabMPI.
  %addpath([PMATLAB_HOME '/src']);              % Add pMatlab.
  %pMatlabGlobalsInit;
  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  
disp('ok')