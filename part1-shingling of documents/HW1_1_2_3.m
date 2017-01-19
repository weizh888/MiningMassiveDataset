clear all;
clc;

fid = fopen('1.2.2_count.txt','r');
data = textscan(fid, '%f %f', 'HeaderLines', 0, 'CollectOutput', 1);
data = cell2mat(data);

data_x = data(:,1);
data_y = data(:,2);

figure;
subplot(1,2,1)
% using all data for fitting
a1 = 0.981599; c1 = 7857.14;
a2 = 0.881837; c2 = 5851.22;
a3 = 0.450842; c3 = 4369.64;

fit1_y = c1*data_x.^(-a1);
fit2_y = c2*data_x.^(-a2);
fit3_y = c3*data_x.^(-a3);
plot(data_x,data_y,'k.',data_x,fit1_y,'-g',data_x,fit2_y,'-r',data_x,fit3_y,'-b')
legend('Raw Data','Fit to 1-Norm', 'Fit to 2-Norm', 'Fit to Inf-Norm')
xlabel('the top x-th word')
ylabel('count of the top x-th word')
title('Using all data points for non-linear fitting')
xlim([0, 50])
ylim([0, 6000])

% using first 50 data points for fitting
subplot(1,2,2)
a1 = 0.853588; c1 = 5412.;
a2 = 0.850784; c2 = 5757.52;
a3 = 0.450842; c3 = 4369.64;

fit1_y = c1*data_x.^(-a1);
fit2_y = c2*data_x.^(-a2);
fit3_y = c3*data_x.^(-a3);
plot(data_x,data_y,'k.',data_x,fit1_y,'-g',data_x,fit2_y,'-r',data_x,fit3_y,'-b')
legend('Raw Data','Fit to 1-Norm', 'Fit to 2-Norm', 'Fit to Inf-Norm')
xlabel('the top x-th word')
ylabel('count of the top x-th word')
title('Using Top 50 points for non-linear fitting')
xlim([0, 50])
ylim([0, 6000])