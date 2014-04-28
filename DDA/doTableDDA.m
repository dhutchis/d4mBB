function doTableDDA(T)
% doTableDDA: Counts (Ni,Vi,Mi) triples for table T and prints classification
% T should be an asociative array or database handle
% Takes T in unexploded (dense) form.
% Please check classification by hand. Uses rough separators.

colmat = Str2mat(Col(T));
header = sprintf('%-*s: %6s,%6s,%6s; %-14s',size(colmat(1,:),2),'Entity',...
    'Ni','Vi','Mi','Type');
fprintf('%s\n%s\n',header,repmat('-',1,nnz(header)));
% For each entity (= column)
for i = 1:size(colmat,1)
    colnamei = colmat(i,:); 
    firstnull = find(colnamei == 0,1,'first');
    if firstnull % if null characters at end
        colnamei = colnamei(1:firstnull-1);
    end
    
    Ei = T(:,colnamei); % select only this column
    if ~nnz(Ei) % skip columns that have no values
        continue;
    end
    Ni = size(Str2mat(Row(Ei)),1); % get number of rows
    Ei_exp = val2col(Ei,'|'); % explode
    Vi = nnz(Ei_exp); % number of values
    Mi = size(Str2mat(Col(Ei_exp)),1); % number of columns
    
    % classify
    if Ni < 100 && Mi < 100 % assumes large data set; 100 arbitrary
        cl = 'Vestigial';
    elseif Ni < Mi / 1.25 
        cl = 'Authoritative';
    elseif abs(Mi - sqrt(Ni)) < abs(Mi - Ni)
        cl = 'Organizational';
    else
        cl = 'Identity';
    end
    
    fprintf('%s: %6d,%6d,%6d; %14s\n',colmat(i,:),Ni,Vi,Mi,cl);
end

end
