from datetime import datetime
from ntpath import join
import cv2
import mediapipe as mp 
import numpy as np
import glob
import os.path
import time
import sys

fname=sys.argv[1]




# dir = '.'
# files = glob.glob(os.path.join(dir, '*.mp4'))


mp_face_mesh=mp.solutions.face_mesh
face_mesh=mp_face_mesh.FaceMesh()
path=os.path.join(sys.argv[1],sys.argv[2])

cap=cv2.VideoCapture(path)
print(cap)
fourcc = cv2.VideoWriter_fourcc(*'XVID')
fout='_'+sys.argv[2]
out = cv2.VideoWriter(os.path.join(sys.argv[3],fout),cv2.VideoWriter_fourcc(*'MP4V'), 20.0, (640,480))
while True is not None:


    ret,frame=cap.read(0)
    try:
        frame = cv2.resize(frame, None, fx=0.5, fy=0.5)
    except:
        break
    copy_frame=frame.copy()
    # image= cv2.imread("person.jpg")
    if ret is not True:
        break
    height,width,_ =frame.shape
    rgb_image=cv2.cvtColor(frame,cv2.COLOR_BGR2RGB)
    res=face_mesh.process(rgb_image)
    frame_res= []

 
    if res.multi_face_landmarks is not None:
        for facial_landmarks in res.multi_face_landmarks:
            for i in  range(0,468):
                pt= facial_landmarks.landmark[i]
                x=int(pt.x * width)
                y=int(pt.y * height)
                frame_res.append((x,y))

                # cv2.circle(frame,(x,y),2,(100,100,0),-1)
            
            convexhull=cv2.convexHull(np.array(frame_res))
            mask=np.zeros((height,width),np.uint8)
            # cv2.polylines(mask,[convexhull],True,255,3)
            cv2.fillConvexPoly(mask,convexhull,255)
            # cv2.imshow("frame",frame)
            copy_frame=cv2.blur(copy_frame,(51,51))
            face_extracted=cv2.bitwise_and(copy_frame,copy_frame,mask=mask)
            blurred_face=cv2.GaussianBlur(face_extracted,(51,51),0)

            background_mask=cv2.bitwise_not(mask)
            background=cv2.bitwise_and(frame,frame,mask=background_mask)
            result=cv2.add(background,face_extracted)
            out.write(cv2.resize(result,(640,480)))



    # cv2.imshow("frame",frame)
    # cv2.imshow("mask",mask)
    # cv2.imshow("mask",blurred_face)
    # cv2.imshow("mask",result)



    # cv2.waitKey(1)
# cap.release()
# out.release()
# print(datetime.now()-begin)
# cv2.destroyAllWindows()

