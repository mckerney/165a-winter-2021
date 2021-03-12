import styled from 'styled-components';
import React, { useState } from 'react';

function Select(props) {
  return (
      <SelectDisplay>
        <label>key: </label>
        <input></input>
        <br/>
        <label>value: </label>
        <input></input>
        <br/>
        <SubmitButton>
         select
        </SubmitButton>
      </SelectDisplay>
  );
}


const SelectDisplay = styled.div`
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

export default Select;
