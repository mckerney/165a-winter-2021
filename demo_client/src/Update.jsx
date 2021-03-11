import styled from 'styled-components';
import React, { useState } from 'react';

function Update(props) {
  return (
      <UpdateDisplay>
        <label>primary key: </label>
        <input></input>
        <br/>
        <div>new values (blank to keep them the same): </div>
        <label>col1: </label>
        <UpdateInput></UpdateInput>
        <label> col2: </label>
        <UpdateInput></UpdateInput>
        <label> col3: </label>
        <UpdateInput></UpdateInput>
        <br/>
        <SubmitButton>
          update
        </SubmitButton>
      </UpdateDisplay>
  );
}

const UpdateInput = styled.input`
width:50px;
`;


const UpdateDisplay = styled.div`
box-shadow:0 30px 50px -30px rgba(0,0,0,.15);
background-color:#fff;
padding:10px;
height:120px;
`;

const SubmitButton = styled.button`
background-color:#0cce6b;
color:'white';
margin:30px 10px 10px 0px;
border:none;
border-radius:4px;
padding:5px;
font-size:15px;

transition:border-color 0.2s, color 0.2s, background-color 0.2s;

:hover{
  background-color:#dbdbdb;
  color:black;
  border-color:#dbdbdb;
}
`;

export default Update;
